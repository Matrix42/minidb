package com.minidb.storage.lsm;

import com.minidb.storage.common.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SSTableReader implements AutoCloseable {
    private static final byte[] MAGIC = "LSMTBL".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LEN_SIZE = 4;

    private final Path file;
    private final TableSchema schema;
    private final PartFormat format;
    private final BufferAllocator allocator;
    private final SSTable metadata;
    private final List<BlockIndex> blockIndex;

    public SSTableReader(
            Path file, TableSchema schema, PartFormat format, BufferAllocator allocator) {
        this.file = file;
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
        try {
            long fileSize = Files.size(file);
            // Read footer: 先读末尾 4 字节取页脚长度,再读页脚内容
            FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
            ByteBuffer lenBuf = ByteBuffer.allocate(FOOTER_LEN_SIZE);
            ch.position(fileSize - FOOTER_LEN_SIZE);
            readFully(ch, lenBuf);
            lenBuf.flip();
            int footerLen = lenBuf.getInt();
            ByteBuffer footerBuf = ByteBuffer.allocate(footerLen);
            ch.position(fileSize - FOOTER_LEN_SIZE - footerLen);
            readFully(ch, footerBuf);
            footerBuf.flip();
            byte[] magic = new byte[6];
            footerBuf.get(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("not a valid SSTable: " + file);
            }
            int level = footerBuf.get();
            // 跳过历史格式的 blockCount(4)——读取方不使用它(block 数从 index block 的
            // entryCount 取),但旧版本写入的 .sst 文件含该字段,必须消耗字节保持对齐,
            // 否则 rowCount/minKeyLen 读到错位字节 → decodeKey EOF(重启回归)。
            footerBuf.getInt();
            long rowCount = footerBuf.getLong();
            short minKeyLen = footerBuf.getShort();
            byte[] minKeyBytes = new byte[minKeyLen];
            footerBuf.get(minKeyBytes);
            short maxKeyLen = footerBuf.getShort();
            byte[] maxKeyBytes = new byte[maxKeyLen];
            footerBuf.get(maxKeyBytes);
            long indexOffset = footerBuf.getLong();
            long bloomOffset = footerBuf.getLong();
            ch.close();

            List<Object> minKey = decodeKey(minKeyBytes, schema);
            List<Object> maxKey = decodeKey(maxKeyBytes, schema);

            // Read bloom filter
            ch = FileChannel.open(file, StandardOpenOption.READ);
            // bloom filter 在 index block 之后、footer 之前:
            // [data blocks] [index block] [bloom filter] [footer]
            long bloomSize = fileSize - FOOTER_LEN_SIZE - footerLen - bloomOffset;
            ByteBuffer bloomBuf = ByteBuffer.allocate((int) bloomSize);
            ch.position(bloomOffset);
            readFully(ch, bloomBuf);
            BloomFilter bloom = BloomFilter.fromBytes(bloomBuf.array());

            // Read index block
            ByteBuffer idxBuf = ByteBuffer.allocate((int) (bloomOffset - indexOffset));
            ch.position(indexOffset);
            readFully(ch, idxBuf);
            idxBuf.flip();
            int entryCount = idxBuf.getInt();
            blockIndex = new ArrayList<>(entryCount);
            for (int i = 0; i < entryCount; i++) {
                short kLen = idxBuf.getShort();
                byte[] kBytes = new byte[kLen];
                idxBuf.get(kBytes);
                long offset = idxBuf.getLong();
                int size = idxBuf.getInt();
                blockIndex.add(new BlockIndex(decodeKey(kBytes, schema), offset, size));
            }
            ch.close();

            this.metadata = new SSTable(file, level, 0, minKey, maxKey, rowCount, bloom);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public SSTable metadata() {
        return metadata;
    }

    public BatchIterator scan() {
        return scanWindow(0, blockIndex.size() - 1);
    }

    /**
     * 范围扫描:只读与闭区间 [rangeLo, rangeHi] 相交的 block(块索引 startKey 二分裁剪)。 块内行不过滤(超集语义,调用方按原条件过滤);lo/hi 元素为
     * null 表示该列无界。
     */
    public BatchIterator scan(List<Object> rangeLo, List<Object> rangeHi) {
        // 块 i 的 key 范围 = [startKey[i], startKey[i+1])(最后一块上界 +∞),
        // 与 [lo, hi] 相交 ⟺ startKey[i] <= hi && (i 是最后一块 || startKey[i+1] > lo)。
        int last = lastBlockIndex(rangeHi);
        if (last < 0) {
            return scanWindow(0, -1); // 空窗口
        }
        int first = firstBlockIndex(rangeLo);
        return scanWindow(Math.min(first, last), last);
    }

    /** 最大块下标 last:startKey[last] <= hi;hi 无界或所有块都在 hi 前时为最后一块,所有块都在 hi 后时为 -1。 */
    private int lastBlockIndex(List<Object> hi) {
        int lo = 0;
        int hiIdx = blockIndex.size();
        while (lo < hiIdx) {
            int mid = (lo + hiIdx) >>> 1;
            if (keyLte(blockIndex.get(mid).startKey(), hi)) {
                lo = mid + 1;
            } else {
                hiIdx = mid;
            }
        }
        return lo - 1;
    }

    /** 第一个相交块下标:最小的 i 使 i 是最后一块或 startKey[i+1] > lo。 */
    private int firstBlockIndex(List<Object> lo) {
        // p = 最小 j ∈ [1, n] 使 startKey[j] > lo(最后一块之后视为 +∞);first = p - 1
        int loIdx = 1;
        int hiIdx = blockIndex.size();
        while (loIdx < hiIdx) {
            int mid = (loIdx + hiIdx) >>> 1;
            if (keyGt(blockIndex.get(mid).startKey(), lo)) {
                hiIdx = mid;
            } else {
                loIdx = mid + 1;
            }
        }
        return loIdx - 1;
    }

    /** key <= hi;hi 为 null 或元素为 null(该列无上界)时恒成立。 */
    static boolean keyLte(List<Object> key, List<Object> hi) {
        if (hi == null) {
            return true;
        }
        for (int i = 0; i < hi.size(); i++) {
            Object b = hi.get(i);
            if (b == null) {
                return true;
            }
            int cmp = cmpElement(key.get(i), b);
            if (cmp < 0) {
                return true;
            }
            if (cmp > 0) {
                return false;
            }
        }
        return true;
    }

    /** key >= lo;lo 为 null 或元素为 null(该列无下界)时恒成立。 */
    static boolean keyGte(List<Object> key, List<Object> lo) {
        if (lo == null) {
            return true;
        }
        for (int i = 0; i < lo.size(); i++) {
            Object b = lo.get(i);
            if (b == null) {
                return true;
            }
            int cmp = cmpElement(key.get(i), b);
            if (cmp > 0) {
                return true;
            }
            if (cmp < 0) {
                return false;
            }
        }
        return true;
    }

    /** key > lo(严格);lo 为 null 或元素为 null 时恒成立。 */
    static boolean keyGt(List<Object> key, List<Object> lo) {
        if (lo == null) {
            return true;
        }
        for (int i = 0; i < lo.size(); i++) {
            Object b = lo.get(i);
            if (b == null) {
                return true;
            }
            int cmp = cmpElement(key.get(i), b);
            if (cmp > 0) {
                return true;
            }
            if (cmp < 0) {
                return false;
            }
        }
        return false; // key == lo
    }

    /** 与 {@code SSTable.CMP} 同语义:Number 按 long 值比较,其余按 raw Comparable。 */
    private static int cmpElement(Object a, Object b) {
        if (a instanceof Number an && b instanceof Number bn) {
            return Long.compare(an.longValue(), bn.longValue());
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        int cmp = ((Comparable) a).compareTo(b);
        return cmp;
    }

    /** 只读 [first, last] 块区间的扫描迭代器(空窗口 last < first 时无输出)。 */
    private BatchIterator scanWindow(int first, int last) {
        return new BatchIterator() {
            int idx = first;
            // 文件句柄整个扫描只开一次,块间靠 position 定位(原实现每块重开文件)
            final FileChannel channel;
            final List<VectorSchemaRoot> read = new ArrayList<>();

            {
                try {
                    this.channel = FileChannel.open(file, StandardOpenOption.READ);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return idx <= last;
            }

            @Override
            public VectorSchemaRoot next() {
                BlockIndex bi = blockIndex.get(idx++);
                try {
                    // 块头 = rows(2) + dataLen(4)。rows 计数读取方不使用(行数从 Arrow
                    // 数据恢复),但旧版本写入的 .sst 文件含该字段,必须消耗字节保持对齐。
                    ByteBuffer header = ByteBuffer.allocate(6);
                    channel.position(bi.offset);
                    readFully(channel, header);
                    header.flip();
                    header.getShort();
                    int dataLen = header.getInt();
                    ByteBuffer data = ByteBuffer.allocate(dataLen);
                    readFully(channel, data);

                    // 块字节直接内存解码,不落临时文件(format.read(byte[]) 内存读)
                    VectorSchemaRoot root =
                            format.read(data.array(), ArrowTypes.arrowSchema(schema), allocator);
                    read.add(root);
                    return root;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void close() {
                try {
                    channel.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                for (VectorSchemaRoot batch : read) {
                    batch.close();
                }
                read.clear();
            }
        };
    }

    @Override
    public void close() {
        // no persistent state
    }

    private static void readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (ch.read(buf) < 0) throw new EOFException();
        }
    }

    /** 解码 key 字节数组(encodeKey 的逆):按主键列类型逐列读回。 整数读翻转符号位后还原,其余列读长度前缀 + UTF-8 字符串。 */
    static List<Object> decodeKey(byte[] keyBytes, TableSchema schema) {
        List<Object> key = new ArrayList<>();
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(keyBytes));
            for (String pkCol : schema.primaryKey()) {
                ColumnType t = schema.columns().get(schema.columnIndex(pkCol)).type();
                switch (t) {
                    case SMALLINT -> key.add((short) (dis.readShort() ^ 0x8000));
                    case INTEGER -> key.add(dis.readInt() ^ 0x8000_0000);
                    case BIGINT -> key.add(dis.readLong() ^ 0x8000_0000_0000_0000L);
                    default -> {
                        // 其余列(含 DATE/TIMESTAMP 的 LocalDate/LocalDateTime 值)走长度前缀 + UTF-8
                        byte[] bytes = new byte[dis.readInt()];
                        dis.readFully(bytes);
                        key.add(new String(bytes, StandardCharsets.UTF_8));
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return key;
    }

    private record BlockIndex(List<Object> startKey, long offset, int size) {}
}
