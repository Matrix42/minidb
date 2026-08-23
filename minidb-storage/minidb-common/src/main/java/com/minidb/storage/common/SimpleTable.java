package com.minidb.storage.common;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * 一张表:一个目录,目录(可嵌套)里是若干 part 文件。数据不驻留内存——写入把 batch 直接
 * 落成一个新 part,读取递归读所有 part 逐个返回 batch(用完释放)。part 的物理编码由
 * {@link PartFormat} 决定(arrow/parquet),本类只负责目录组织与分段。
 */
public class SimpleTable implements TableHandle {

    /** compaction 交换目录的后缀:新 part 先写 .tmp,交换时旧目录暂存 .bak。 */
    public static final String COMPACT_TMP_SUFFIX = ".compact.tmp";
    public static final String COMPACT_BACKUP_SUFFIX = ".compact.bak";

    private final TableSchema schema;
    private final BufferAllocator allocator;
    private final Path tableDir;
    private final PartFormat format;
    private final Schema arrowSchema;
    private final AtomicInteger partSeq;

    public SimpleTable(TableSchema schema, BufferAllocator allocator, Path tableDir,
                       PartFormat format) {
        this.schema = schema;
        this.allocator = allocator;
        this.tableDir = tableDir;
        this.format = format;
        createTableDir();
        List<Field> fields = new ArrayList<>();
        for (ColumnMeta column : schema.columns()) {
            fields.add(ArrowTypes.field(column));
        }
        this.arrowSchema = new Schema(fields, Map.of("schema", schema.schemaName()));
        this.partSeq = new AtomicInteger(maxPartSeq());
    }

    /** 建表时确保表目录存在(幂等);数据本身仍由 writePart 按需落盘。 */
    private void createTableDir() {
        try {
            Files.createDirectories(tableDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TableSchema schema() {
        return schema;
    }

    public Schema arrowSchema() {
        return arrowSchema;
    }

    public VectorSchemaRoot newBatchRoot() {
        return VectorSchemaRoot.create(arrowSchema, allocator);
    }

    /** 递归读取目录下所有 part 文件,逐个返回 batch。batch 归本迭代器,close 时统一释放。 */
    public BatchIterator scan() {
        return scan(null);
    }

    /** 列裁剪扫描:只读指定列索引。null 或全列索引时回退全量扫描。 */
    public BatchIterator scan(int[] projectedColumns) {
        if (projectedColumns == null || projectedColumns.length == arrowSchema.getFields().size()) {
            return scanAll();
        }
        // 验证投影列索引有效(ALTER TABLE 可能增列,plan 的投影列可能过时)
        for (int col : projectedColumns) {
            if (col < 0 || col >= arrowSchema.getFields().size()) {
                return scanAll();
            }
        }
        return scanAll(projectedColumns);
    }

    private BatchIterator scanAll() {
        return scanAll(null);
    }

    private BatchIterator scanAll(int[] projectedColumns) {
        List<Path> parts = partFiles();
        return new BatchIterator() {
            int idx = 0;
            final List<VectorSchemaRoot> read = new ArrayList<>();

            @Override
            public boolean hasNext() {
                return idx < parts.size();
            }

            @Override
            public VectorSchemaRoot next() {
                VectorSchemaRoot batch = projectedColumns == null
                        ? format.read(parts.get(idx++), arrowSchema, allocator)
                        : format.read(parts.get(idx++), arrowSchema, allocator, projectedColumns);
                read.add(batch);
                return batch;
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

    /** 把一个 batch 直接落成一个新 part 文件。 */
    public void writePart(VectorSchemaRoot batch) {
        int seq = partSeq.incrementAndGet();
        format.write(tableDir.resolve(String.format("part-%06d.%s", seq, format.fileExtension())), batch);
    }

    @Override
    public void writePart(VectorSchemaRoot batch, TableHandle.Operation op) {
        writePart(batch);
    }

    /** 删除所有 part 文件(truncate)。 */
    public void clearParts() {
        for (Path part : partFiles()) {
            try {
                Files.deleteIfExists(part);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * 合并所有 part:按大小(近似 buffer 字节和)切分,目标单个 part ≤ {@code targetSizeBytes}。
     * 合并后 part 文件被整体替换;交换是「旧目录 → .bak、.tmp → 表目录、删 .bak」的原子改名,
     * 任意时刻崩溃要么是旧数据要么是新数据(不会混读或空目录)。
     *
     * @return 合并后的 part 数
     */
    public int compact(long targetSizeBytes) {
        cleanupStaleArtifacts();
        List<Path> parts = partFiles();
        if (parts.size() <= 1) {
            return parts.size();
        }
        Path tmpDir = sibling(COMPACT_TMP_SUFFIX);
        Path bakDir = sibling(COMPACT_BACKUP_SUFFIX);
        try {
            Files.createDirectories(tmpDir);
            int newCount = mergeInto(parts, tmpDir, targetSizeBytes);
            Files.move(tableDir, bakDir);
            try {
                Files.move(tmpDir, tableDir);
            } catch (IOException e) {
                // 交换第二步失败:回滚,恢复旧目录。
                Files.move(bakDir, tableDir);
                throw e;
            }
            deleteRecursively(bakDir);
            partSeq.set(maxPartSeq());
            return newCount;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 清理上次 compaction 中断留下的交换目录(旧目录仍完整)。 */
    private void cleanupStaleArtifacts() {
        Path tmpDir = sibling(COMPACT_TMP_SUFFIX);
        Path bakDir = sibling(COMPACT_BACKUP_SUFFIX);
        if (Files.exists(tmpDir)) {
            deleteRecursively(tmpDir);
        }
        if (Files.exists(bakDir)) {
            if (Files.exists(tableDir)) {
                deleteRecursively(bakDir); // 上次交换已完成,旧目录可删
            } else {
                moveOrThrow(bakDir, tableDir); // 上次交换中断,回滚旧目录
            }
        }
    }

    private int mergeInto(List<Path> parts, Path destDir, long targetSizeBytes) {
        int seq = 0;
        VectorSchemaRoot out = null;
        int outRows = 0;
        try {
            for (Path part : parts) {
                VectorSchemaRoot batch = format.read(part, arrowSchema, allocator);
                try {
                    for (int r = 0; r < batch.getRowCount(); r++) {
                        if (out == null) {
                            out = newBatchRoot();
                            out.allocateNew();
                            outRows = 0;
                        }
                        copyRow(batch, r, out, outRows);
                        outRows++;
                        if (estimatedBytes(out) >= targetSizeBytes) {
                            flushPart(out, outRows, destDir, ++seq);
                            out.close();
                            out = null;
                        }
                    }
                } finally {
                    batch.close();
                }
            }
            if (out != null) {
                flushPart(out, outRows, destDir, ++seq);
            }
            return seq;
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    private void flushPart(VectorSchemaRoot out, int outRows, Path destDir, int seq) {
        out.setRowCount(outRows);
        format.write(destDir.resolve(String.format("part-%06d.%s", seq, format.fileExtension())), out);
    }

    private static void copyRow(VectorSchemaRoot src, int srcRow, VectorSchemaRoot dst, int dstRow) {
        for (int c = 0; c < src.getFieldVectors().size(); c++) {
            dst.getVector(c).copyFromSafe(srcRow, dstRow, src.getVector(c));
        }
    }

    /** 近似估算一个 batch 落盘的字节数(各 buffer 容量和,不真序列化)。 */
    private static long estimatedBytes(VectorSchemaRoot root) {
        long total = 0;
        for (FieldVector v : root.getFieldVectors()) {
            for (ArrowBuf buf : v.getBuffers(false)) {
                if (buf != null) {
                    total += buf.capacity();
                }
            }
        }
        return total;
    }

    private Path sibling(String suffix) {
        return tableDir.resolveSibling(tableDir.getFileName().toString() + suffix);
    }

    private void moveOrThrow(Path src, Path dst) {
        try {
            Files.move(src, dst);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void deleteRecursively(Path dir) {
        if (!Files.exists(dir)) {
            return;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (Files.isDirectory(p)) {
                    deleteRecursively(p);
                } else {
                    Files.deleteIfExists(p);
                }
            }
            Files.deleteIfExists(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long rowCount() {
        long count = 0;
        for (Path part : partFiles()) {
            count += format.rowCount(part, allocator);
        }
        return count;
    }

    /** part 文件数(EXPLAIN 的 batches 列用)。 */
    public int partCount() {
        return partFiles().size();
    }

    /** 递归列出目录下所有 part 文件(按格式扩展名),按名排序保证稳定顺序。 */
    private List<Path> partFiles() {
        List<Path> parts = new ArrayList<>();
        if (Files.exists(tableDir)) {
            collectParts(tableDir, parts);
        }
        parts.sort(Comparator.comparing(p -> p.getFileName().toString()));
        return parts;
    }

    private void collectParts(Path dir, List<Path> parts) {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (Files.isDirectory(p)) {
                    collectParts(p, parts);
                } else if (p.getFileName().toString().endsWith(partSuffix())) {
                    parts.add(p);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 现有 part 文件的最大序号(重启后接续)。 */
    private int maxPartSeq() {
        String suffix = partSuffix();
        int max = 0;
        for (Path part : partFiles()) {
            String name = part.getFileName().toString();
            if (name.startsWith("part-") && name.endsWith(suffix)) {
                try {
                    int seq = Integer.parseInt(name.substring("part-".length(), name.length() - suffix.length()));
                    max = Math.max(max, seq);
                } catch (NumberFormatException ignored) {
                    // 非标准命名,跳过
                }
            }
        }
        return max;
    }

    private String partSuffix() {
        return "." + format.fileExtension();
    }
}
