package com.minidb.server.storage;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.exec.BatchIterator;
import com.minidb.server.exec.RowCopier;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * 一张表:一个目录,目录(可嵌套)里是若干 part 文件(.arrow)。数据不驻留内存——
 * 写入把 batch 直接落成一个新 part 文件,读取递归读所有 part 逐个返回 batch(用完释放)。
 */
public class ArrowTable {

    private final TableSchema schema;
    private final BufferAllocator allocator;
    private final Path tableDir;
    private final Schema arrowSchema;
    private final AtomicInteger partSeq;

    public ArrowTable(TableSchema schema, BufferAllocator allocator, Path tableDir) {
        this.schema = schema;
        this.allocator = allocator;
        this.tableDir = tableDir;
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
                VectorSchemaRoot batch = readPart(parts.get(idx++));
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
        Path part = tableDir.resolve(String.format("part-%06d.arrow", seq));
        try {
            Files.createDirectories(tableDir);
            try (SeekableByteChannel channel = Files.newByteChannel(part,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING);
                 ArrowFileWriter writer = new ArrowFileWriter(batch, null, channel)) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
            count += rowCountOf(part);
        }
        return count;
    }

    /** part 文件数(EXPLAIN 的 batches 列用)。 */
    public int partCount() {
        return partFiles().size();
    }

    private VectorSchemaRoot readPart(Path part) {
        try (SeekableByteChannel channel = Files.newByteChannel(part, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            VectorSchemaRoot out = VectorSchemaRoot.create(arrowSchema, allocator);
            out.allocateNew();
            int dst = 0;
            while (reader.loadNextBatch()) {
                int batchRows = src.getRowCount();
                for (int i = 0; i < batchRows; i++) {
                    RowCopier.copyRow(src, i, out, dst + i);
                }
                dst += batchRows;
            }
            out.setRowCount(dst);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long rowCountOf(Path part) {
        try (SeekableByteChannel channel = Files.newByteChannel(part, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            long count = 0;
            while (reader.loadNextBatch()) {
                count += reader.getVectorSchemaRoot().getRowCount();
            }
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 递归列出目录下所有 .arrow 文件,按名排序保证稳定顺序。 */
    private List<Path> partFiles() {
        List<Path> parts = new ArrayList<>();
        if (Files.exists(tableDir)) {
            collectParts(tableDir, parts);
        }
        parts.sort(Comparator.comparing(p -> p.getFileName().toString()));
        return parts;
    }

    private static void collectParts(Path dir, List<Path> parts) {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (Files.isDirectory(p)) {
                    collectParts(p, parts);
                } else if (p.getFileName().toString().endsWith(".arrow")) {
                    parts.add(p);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 现有 part 文件的最大序号(重启后接续)。 */
    private int maxPartSeq() {
        int max = 0;
        for (Path part : partFiles()) {
            String name = part.getFileName().toString();
            if (name.startsWith("part-") && name.endsWith(".arrow")) {
                try {
                    int seq = Integer.parseInt(name.substring("part-".length(), name.length() - ".arrow".length()));
                    max = Math.max(max, seq);
                } catch (NumberFormatException ignored) {
                    // 非标准命名,跳过
                }
            }
        }
        return max;
    }
}
