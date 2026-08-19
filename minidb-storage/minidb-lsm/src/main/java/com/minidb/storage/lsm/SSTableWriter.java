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
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class SSTableWriter {
    private static final int BLOCK_SIZE = 64 * 1024; // 64KB
    private static final byte[] MAGIC = "LSMTBL".getBytes(StandardCharsets.UTF_8);
    // 页脚变长,末尾 4 字节存页脚长度(不含这 4 字节自身)
    private static final int FOOTER_LEN_SIZE = 4;

    private final Path file;
    private final int level;
    private final TableSchema schema;
    private final PartFormat format;
    private final BufferAllocator allocator;
    private final int bloomBitsPerKey;

    public SSTableWriter(Path file, int level, TableSchema schema,
                         PartFormat format, BufferAllocator allocator, int bloomBitsPerKey) {
        this.file = file;
        this.level = level;
        this.schema = schema;
        this.format = format;
        this.allocator = allocator;
        this.bloomBitsPerKey = bloomBitsPerKey;
    }

    public long writeFromMemTable(MemTable mt) {
        return writeFromIterator(mt.iterator(), mt.size());
    }

    public long writeFromIterator(
            java.util.Iterator<Map.Entry<List<Object>, RowValue>> iterator, int totalRows) {
        BloomFilter bloom = new BloomFilter(bloomBitsPerKey, Math.max(1, totalRows));
        List<Object> minKey = null;
        List<Object> maxKey = null;
        long rowCount = 0;
        List<BlockInfo> blockInfos = new ArrayList<>();

        try {
            Files.createDirectories(file.getParent());
            try (FileChannel ch = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                List<Object[]> blockRows = new ArrayList<>();
                List<Object> blockStartKey = null;

                while (iterator.hasNext()) {
                    var entry = iterator.next();
                    List<Object> key = entry.getKey();
                    RowValue rv = entry.getValue();
                    rowCount++;

                    if (minKey == null) minKey = key;
                    maxKey = key;

                    bloom.add(encodeKey(key));

                    if (blockStartKey == null) blockStartKey = key;
                    blockRows.add(rv.values());

                    if (estimatedBlockBytes(blockRows) >= BLOCK_SIZE) {
                        BlockInfo bi = writeBlock(ch, blockStartKey, blockRows);
                        blockInfos.add(bi);
                        blockRows.clear();
                        blockStartKey = null;
                    }
                }
                // 尾块
                if (!blockRows.isEmpty()) {
                    BlockInfo bi = writeBlock(ch, blockStartKey, blockRows);
                    blockInfos.add(bi);
                }

                // Index block: 先计算总大小再分配缓冲区,避免 key 超长溢出
                long indexOffset = ch.position();
                int idxSize = 4; // entryCount int
                for (BlockInfo bi : blockInfos) {
                    byte[] keyBytes = encodeKey(bi.startKey);
                    idxSize += 2 + keyBytes.length + 8 + 4; // short keyLen + bytes + long offset + int size
                }
                ByteBuffer idxBuf = ByteBuffer.allocate(idxSize);
                idxBuf.putInt(blockInfos.size());
                for (BlockInfo bi : blockInfos) {
                    byte[] keyBytes = encodeKey(bi.startKey);
                    idxBuf.putShort((short) keyBytes.length);
                    idxBuf.put(keyBytes);
                    idxBuf.putLong(bi.offset);
                    idxBuf.putInt(bi.size);
                }
                idxBuf.flip();
                while (idxBuf.hasRemaining()) ch.write(idxBuf);

                // Bloom filter
                long bloomOffset = ch.position();
                byte[] bloomBytes = bloom.toBytes();
                ByteBuffer bloomBuf = ByteBuffer.wrap(bloomBytes);
                while (bloomBuf.hasRemaining()) ch.write(bloomBuf);

                // Footer
                writeFooter(ch, blockInfos.size(), rowCount,
                        minKey, maxKey, indexOffset, bloomOffset);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return rowCount;
    }

    private BlockInfo writeBlock(FileChannel ch, List<Object> startKey,
                                  List<Object[]> rows) throws IOException {
        long offset = ch.position();
        // 把 rows 转成 VectorSchemaRoot 然后用 PartFormat 编码
        VectorSchemaRoot root = rowsToRoot(rows);
        try {
            // 暂时写到一个临时文件，然后读回字节
            Path tmp = Files.createTempFile("block", ".tmp");
            try {
                format.write(tmp, root);
                byte[] blockBytes = Files.readAllBytes(tmp);
                ByteBuffer buf = ByteBuffer.allocate(2 + 4 + blockBytes.length);
                buf.putShort((short) rows.size());
                buf.putInt(blockBytes.length);
                buf.put(blockBytes);
                buf.flip();
                while (buf.hasRemaining()) ch.write(buf);
                int size = 2 + 4 + blockBytes.length;
                return new BlockInfo(startKey, offset, size);
            } finally {
                Files.deleteIfExists(tmp);
            }
        } finally {
            root.close();
        }
    }

    private VectorSchemaRoot rowsToRoot(List<Object[]> rows) {
        VectorSchemaRoot root = VectorSchemaRoot.create(
                ArrowTypes.arrowSchema(schema), allocator);
        root.allocateNew();
        for (int i = 0; i < rows.size(); i++) {
            writeRow(root, i, rows.get(i));
        }
        root.setRowCount(rows.size());
        return root;
    }

    private void writeRow(VectorSchemaRoot root, int row, Object[] values) {
        writeRow(root, row, values, schema);
    }

    /** 将行值写入 VectorSchemaRoot 的指定行（供 MergeIterator 等外部使用） */
    public static void writeRow(VectorSchemaRoot root, int row, Object[] values, TableSchema schema) {
        List<ColumnMeta> cols = schema.columns();
        for (int c = 0; c < cols.size(); c++) {
            var vector = root.getVector(c);
            Object val = values[c];
            if (val == null) {
                vector.setNull(row);
            } else {
                setVectorValue(vector, row, val, cols.get(c).type());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public static void setVectorValue(
            org.apache.arrow.vector.FieldVector vector, int row, Object val, ColumnType type) {
        switch (type) {
            case INTEGER:
                ((org.apache.arrow.vector.IntVector) vector).setSafe(row, ((Number) val).intValue());
                break;
            case BIGINT:
                ((org.apache.arrow.vector.BigIntVector) vector).setSafe(row, ((Number) val).longValue());
                break;
            case DOUBLE:
                ((org.apache.arrow.vector.Float8Vector) vector).setSafe(row, ((Number) val).doubleValue());
                break;
            case VARCHAR:
                byte[] bytes = val.toString().getBytes(StandardCharsets.UTF_8);
                ((org.apache.arrow.vector.VarCharVector) vector).setSafe(row, bytes);
                break;
            case BOOLEAN:
                ((org.apache.arrow.vector.BitVector) vector).setSafe(row,
                        ((Boolean) val) ? 1 : 0);
                break;
            default:
                throw new UnsupportedOperationException("unsupported type: " + type);
        }
    }

    private long estimatedBlockBytes(List<Object[]> rows) {
        long total = 0;
        for (Object[] row : rows) {
            for (Object v : row) {
                total += v == null ? 8 : (v instanceof String s ? 24 + s.length() * 2L : 16);
            }
        }
        return total;
    }

    private void writeFooter(FileChannel ch, int blockCount, long rowCount,
                              List<Object> minKey, List<Object> maxKey,
                              long indexOffset, long bloomOffset) throws IOException {
        byte[] minBytes = encodeKey(minKey);
        byte[] maxBytes = encodeKey(maxKey);
        // 页脚内容: magic(6) + level(1) + blockCount(4) + rowCount(8)
        //   + minKeyLen(2) + minBytes + maxKeyLen(2) + maxBytes
        //   + indexOffset(8) + bloomOffset(8)
        int footerLen = 6 + 1 + 4 + 8 + 2 + minBytes.length + 2 + maxBytes.length + 8 + 8;
        ByteBuffer footer = ByteBuffer.allocate(footerLen + FOOTER_LEN_SIZE);
        footer.put(MAGIC);
        footer.put((byte) level);
        footer.putInt(blockCount);
        footer.putLong(rowCount);
        footer.putShort((short) minBytes.length);
        footer.put(minBytes);
        footer.putShort((short) maxBytes.length);
        footer.put(maxBytes);
        footer.putLong(indexOffset);
        footer.putLong(bloomOffset);
        // 末尾 4 字节: 页脚长度(不含自身)
        footer.putInt(footerLen);
        footer.flip();
        while (footer.hasRemaining()) ch.write(footer);
    }

    /** 将 key 编码为字节数组(用于索引和 bloom filter)。
     *  整数 key 零填充到 20 位,保证字典序与数值序一致。 */
    static byte[] encodeKey(List<Object> key) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < key.size(); i++) {
            if (i > 0) sb.append('\0');
            sb.append(encodeKeyValue(key.get(i)));
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** 单个 key 值的编码:整数零填充,其余 toString。 */
    static String encodeKeyValue(Object k) {
        if (k instanceof Integer) {
            return String.format("%020d", (Integer) k);
        }
        if (k instanceof Long) {
            return String.format("%020d", (Long) k);
        }
        return k.toString();
    }

    private record BlockInfo(List<Object> startKey, long offset, int size) {}
}