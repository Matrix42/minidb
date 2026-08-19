package com.minidb.storage.lsm;

import com.minidb.storage.common.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class SSTableReader implements AutoCloseable {
    private static final byte[] MAGIC = "LSMTBL".getBytes(StandardCharsets.UTF_8);
    private static final int FOOTER_LEN_SIZE = 4;

    private final Path file;
    private final TableSchema schema;
    private final PartFormat format;
    private final BufferAllocator allocator;
    private final SSTable metadata;
    private final List<BlockIndex> blockIndex;

    public SSTableReader(Path file, TableSchema schema, PartFormat format,
                         BufferAllocator allocator) {
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
            if (!java.util.Arrays.equals(magic, MAGIC)) {
                throw new IllegalArgumentException("not a valid SSTable: " + file);
            }
            int level = footerBuf.get();
            int blockCount = footerBuf.getInt();
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

            List<Object> minKey = decodeKey(minKeyBytes);
            List<Object> maxKey = decodeKey(maxKeyBytes);

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
                blockIndex.add(new BlockIndex(decodeKey(kBytes), offset, size));
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
        return new BatchIterator() {
            int idx = 0;
            final List<VectorSchemaRoot> read = new ArrayList<>();

            @Override
            public boolean hasNext() {
                return idx < blockIndex.size();
            }

            @Override
            public VectorSchemaRoot next() {
                BlockIndex bi = blockIndex.get(idx++);
                try {
                    FileChannel ch = FileChannel.open(file, StandardOpenOption.READ);
                    ByteBuffer header = ByteBuffer.allocate(6);
                    ch.position(bi.offset);
                    readFully(ch, header);
                    header.flip();
                    int rows = Short.toUnsignedInt(header.getShort());
                    int dataLen = header.getInt();
                    ByteBuffer data = ByteBuffer.allocate(dataLen);
                    readFully(ch, data);
                    ch.close();

                    // 写临时文件，用 PartFormat 读回
                    Path tmp = Files.createTempFile("sstblock", ".tmp");
                    Files.write(tmp, data.array());
                    VectorSchemaRoot root = format.read(tmp,
                            ArrowTypes.arrowSchema(schema), allocator);
                    Files.deleteIfExists(tmp);
                    read.add(root);
                    return root;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void close() {
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

    /** 解码 key 字节数组,零填充整数还原为 Integer,其余为 String。 */
    static List<Object> decodeKey(byte[] keyBytes) {
        String s = new String(keyBytes, StandardCharsets.UTF_8);
        String[] parts = s.split("\0", -1);
        List<Object> key = new ArrayList<>(parts.length);
        for (String part : parts) {
            key.add(decodeKeyValue(part));
        }
        return key;
    }

    /** 单个 key 值的解码:尝试解析整数(兼容零填充),否则保留字符串。
     *  encodeKey 对 Integer/Long 都零填充到 20 位,这里先试 int 再试 long。 */
    static Object decodeKeyValue(String part) {
        try {
            return Integer.parseInt(part);
        } catch (NumberFormatException e) {
            try {
                long v = Long.parseLong(part);
                // 值在 int 范围内返回 Integer,否则返回 Long
                if (v >= Integer.MIN_VALUE && v <= Integer.MAX_VALUE) {
                    return (int) v;
                }
                return v;
            } catch (NumberFormatException e2) {
                return part;
            }
        }
    }

    private record BlockIndex(List<Object> startKey, long offset, int size) {}
}