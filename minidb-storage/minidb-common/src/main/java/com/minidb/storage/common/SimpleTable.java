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
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * 一张表:一个目录,目录(可嵌套)里是若干 part 文件。数据不驻留内存——写入把 batch 直接
 * 落成一个新 part,读取递归读所有 part 逐个返回 batch(用完释放)。part 的物理编码由
 * {@link PartFormat} 决定(arrow/parquet),本类只负责目录组织与分段。
 */
public class SimpleTable {

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
        List<Field> fields = new ArrayList<>();
        for (ColumnMeta column : schema.columns()) {
            fields.add(ArrowTypes.field(column));
        }
        this.arrowSchema = new Schema(fields, Map.of("schema", schema.schemaName()));
        this.partSeq = new AtomicInteger(maxPartSeq());
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
                VectorSchemaRoot batch = format.read(parts.get(idx++), arrowSchema, allocator);
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
