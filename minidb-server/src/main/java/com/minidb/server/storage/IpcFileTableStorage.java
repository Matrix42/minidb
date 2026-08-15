package com.minidb.server.storage;

import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.TableSchema;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Arrow IPC 文件存储引擎:每表一个 {@code data/<schema>/<table>.arrow} 文件,整表全量读写。
 * 这是 MiniDB 当前的默认落盘方式——内存态 Arrow 与文件同构,flush/load 近乎零转换。
 */
public class IpcFileTableStorage implements TableStorage {

    private final Path dataDir;

    public IpcFileTableStorage(Path dataDir) {
        this.dataDir = dataDir;
    }

    @Override
    public List<TableRef> listTables() {
        List<TableRef> refs = new ArrayList<>();
        if (!Files.exists(dataDir)) {
            return refs;
        }
        try (DirectoryStream<Path> schemaDirs = Files.newDirectoryStream(dataDir)) {
            for (Path schemaDir : schemaDirs) {
                if (!Files.isDirectory(schemaDir)) {
                    continue;
                }
                String schemaName = schemaDir.getFileName().toString();
                try (DirectoryStream<Path> files = Files.newDirectoryStream(schemaDir, "*.arrow")) {
                    for (Path file : files) {
                        refs.add(new TableRef(schemaName,
                                stripExtension(file.getFileName().toString())));
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return refs;
    }

    @Override
    public LoadedTable load(String schemaName, String tableName, TableSchema schema,
                            BufferAllocator allocator) {
        Path file = dataDir.resolve(fileName(schemaName, tableName));
        try (SeekableByteChannel channel =
                     Files.newByteChannel(file, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            TableSchema resolved = schema != null
                    ? schema
                    : toTableSchema(root.getSchema(), schemaName, tableName);
            ArrowTable table = new ArrowTable(resolved, allocator);
            while (reader.loadNextBatch()) {
                VectorSchemaRoot copy = table.newBatchRoot();
                ArrowRecordBatch recordBatch = new VectorUnloader(root).getRecordBatch();
                new VectorLoader(copy).load(recordBatch);
                recordBatch.close();
                table.appendBatch(copy);
            }
            return new LoadedTable(table, resolved);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void save(String schemaName, String tableName, ArrowTable table) {
        Path file = dataDir.resolve(fileName(schemaName, tableName));
        try {
            Files.createDirectories(file.getParent());
            try (SeekableByteChannel channel = Files.newByteChannel(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                VectorSchemaRoot sink = table.newBatchRoot();
                try (ArrowFileWriter writer = new ArrowFileWriter(sink, null, channel)) {
                    writer.start();
                    for (VectorSchemaRoot batch : table.batches()) {
                        ArrowRecordBatch recordBatch = new VectorUnloader(batch).getRecordBatch();
                        new VectorLoader(sink).load(recordBatch);
                        recordBatch.close();
                        writer.writeBatch();
                    }
                    writer.end();
                } finally {
                    sink.close();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void delete(String schemaName, String tableName) {
        try {
            Files.deleteIfExists(dataDir.resolve(fileName(schemaName, tableName)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void deleteSchema(String schemaName) {
        try {
            Path schemaDir = dataDir.resolve(key(schemaName));
            if (Files.exists(schemaDir)) {
                try (DirectoryStream<Path> ds = Files.newDirectoryStream(schemaDir)) {
                    for (Path p : ds) {
                        Files.deleteIfExists(p);
                    }
                }
                Files.deleteIfExists(schemaDir);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String stripExtension(String fileName) {
        int dot = fileName.lastIndexOf('.');
        return dot < 0 ? fileName : fileName.substring(0, dot);
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    private static String fileName(String schemaName, String tableName) {
        return key(schemaName) + "/" + key(tableName) + ".arrow";
    }

    private static TableSchema toTableSchema(
            org.apache.arrow.vector.types.pojo.Schema arrowSchema,
            String schemaName, String tableName) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (Field field : arrowSchema.getFields()) {
            columns.add(toColumnMeta(field));
        }
        String resolvedSchema = schemaName;
        Map<String, String> meta = arrowSchema.getCustomMetadata();
        if (meta != null && meta.containsKey("schema")) {
            resolvedSchema = meta.get("schema");
        }
        return new TableSchema(resolvedSchema, tableName, columns);
    }

    private static ColumnMeta toColumnMeta(Field field) {
        Map<String, String> meta = field.getMetadata();
        String typeName = meta != null ? meta.get(ArrowTypes.TYPE_NAME_METADATA) : null;
        if (typeName != null) {
            ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
            if (type == ColumnType.DECIMAL || type == ColumnType.NUMERIC) {
                ArrowType.Decimal d = (ArrowType.Decimal) field.getType();
                return new ColumnMeta(field.getName(), type, d.getPrecision(), d.getScale());
            }
            return new ColumnMeta(field.getName(), type);
        }
        // 旧文件无元数据:回退到 Arrow 类型推断。
        return new ColumnMeta(field.getName(), inferFromArrowType(field.getType()));
    }

    private static ColumnType inferFromArrowType(ArrowType type) {
        switch (type.getTypeID()) {
            case Int: {
                ArrowType.Int intType = (ArrowType.Int) type;
                int w = intType.getBitWidth();
                if (w == 16) {
                    return ColumnType.SMALLINT;
                }
                return w == 32 ? ColumnType.INTEGER : ColumnType.BIGINT;
            }
            case FloatingPoint:
                return ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.SINGLE
                        ? ColumnType.REAL : ColumnType.DOUBLE;
            case Decimal:
                // 旧格式(无元数据)的 Decimal 无法区分 DECIMAL/NUMERIC,统一归 DECIMAL;
                // precision/scale 由 ColumnMeta 默认 UNSET,toCalciteType 补默认值。
                return ColumnType.DECIMAL;
            case Utf8:
                return ColumnType.VARCHAR;
            case Bool:
                return ColumnType.BOOLEAN;
            case Date:
                return ColumnType.DATE;
            case Time:
                return ColumnType.TIME;
            case Timestamp:
                return ColumnType.TIMESTAMP;
            case Binary:
                return ColumnType.VARBINARY;
            default:
                throw new IllegalArgumentException(
                        "unsupported arrow type in file: " + type);
        }
    }
}
