package com.minidb.storage.parquet;

import com.minidb.storage.common.PartFormat;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Apache Parquet 格式的 part 读写。
 *
 * <p>parquet-arrow 1.18 只保留 Arrow↔Parquet 的 schema 映射({@link SchemaConverter}),
 * 数据转换的 {@code ArrowWriter}/{@code ArrowRecordConverter} 已移除,故此处手写
 * Arrow 向量 ↔ {@link Group} 的行级转换(逐类型 instanceof 分发)。
 *
 * <p>用 parquet-common 的 {@link InputFile}/{@link OutputFile} 走 Hadoop-free 路径
 * (parquet-hadoop 的 hadoop 依赖是 provided scope,运行时无 hadoop-common)。压缩用
 * UNCOMPRESSED,避免引入编解码器依赖;读写结果与 Arrow 格式逐值等价。
 */
public class ParquetPartFormat implements PartFormat {

    @Override
    public String fileExtension() {
        return "parquet";
    }

    @Override
    public void write(Path part, VectorSchemaRoot batch) {
        try {
            Files.createDirectories(part.getParent());
            writeInto(new NioOutputFile(part), batch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] writeToBytes(VectorSchemaRoot batch) {
        ByteArrayOutputFile out = new ByteArrayOutputFile();
        try {
            writeInto(out, batch);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    /** 把 batch 编码为 parquet 写入给定 OutputFile(文件或内存,见 Nio/ByteArray 实现)。 */
    private static void writeInto(OutputFile outputFile, VectorSchemaRoot batch) throws IOException {
        MessageType parquetSchema = new SchemaConverter().fromArrow(batch.getSchema()).getParquetSchema();
        SimpleGroupFactory groups = new SimpleGroupFactory(parquetSchema);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outputFile)
                .withType(parquetSchema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED);
        try (ParquetWriter<Group> writer = builder.build()) {
            int rows = batch.getRowCount();
            List<FieldVector> vectors = batch.getFieldVectors();
            for (int r = 0; r < rows; r++) {
                Group group = groups.newGroup();
                for (int c = 0; c < vectors.size(); c++) {
                    FieldVector vector = vectors.get(c);
                    if (!vector.isNull(r)) {
                        writeValue(group, c, r, vector);
                    }
                }
                writer.write(group);
            }
        }
    }

    @Override
    public VectorSchemaRoot read(Path part, Schema schema, BufferAllocator allocator) {
        return read(part, schema, allocator, null);
    }

    @Override
    public VectorSchemaRoot read(Path part, Schema schema, BufferAllocator allocator,
                                   int[] projectedColumns) {
        try (ParquetFileReader reader = openReader(part)) {
            return readAll(reader, schema, allocator, projectedColumns);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public VectorSchemaRoot read(byte[] data, Schema schema, BufferAllocator allocator) {
        try (ParquetFileReader reader = ParquetFileReader.open(
                new ByteArrayInputFile(data), options())) {
            return readAll(reader, schema, allocator, null);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 从已打开的 reader 读全部行组为一个 batch(列裁剪见 projectedColumns)。 */
    private VectorSchemaRoot readAll(ParquetFileReader reader, Schema schema,
                                     BufferAllocator allocator, int[] projectedColumns) throws IOException {
        MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
        // 列裁剪:构建投影 schema 和 Parquet 列路径
        Schema projSchema;
        List<String> columnPaths;
        if (projectedColumns == null || projectedColumns.length == schema.getFields().size()) {
            projSchema = schema;
            columnPaths = null; // 全量读
        } else {
            List<Field> projFields = new ArrayList<>();
            columnPaths = new ArrayList<>();
            for (int col : projectedColumns) {
                projFields.add(schema.getFields().get(col));
                columnPaths.add(schema.getFields().get(col).getName());
            }
            projSchema = new Schema(projFields, schema.getCustomMetadata());
        }
        MessageColumnIO columnIO;
        // 投影分支的 Group 结构 schema(SimpleGroup 按此组织列序,必须与投影列一致)
        MessageType groupSchema = parquetSchema;
        if (columnPaths == null) {
            columnIO = new ColumnIOFactory().getColumnIO(parquetSchema, parquetSchema);
        } else {
            List<Type> projParquetFields = new ArrayList<>();
            for (String col : columnPaths) {
                projParquetFields.add(parquetSchema.getType(col));
            }
            MessageType projParquet = new MessageType(parquetSchema.getName(), projParquetFields);
            columnIO = new ColumnIOFactory().getColumnIO(projParquet, parquetSchema);
            groupSchema = projParquet;
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(projSchema, allocator);
        root.allocateNew();
        int dst = 0;
        PageReadStore pages;
        List<FieldVector> vectors = root.getFieldVectors();
        int outCols = vectors.size();
        // record 的 SimpleGroup 结构必须与列裁剪后的 schema 一致(投影分支用 projParquet),
        // 否则列按原 schema 组织、投影位置错位(预存 bug,被 Planner 列裁剪激活)。
        while ((pages = reader.readNextRowGroup()) != null) {
            RecordReader<Group> recordReader =
                    columnIO.getRecordReader(pages, new GroupRecordConverter(groupSchema));
            int rows = (int) pages.getRowCount();
            for (int r = 0; r < rows; r++) {
                Group group = recordReader.read();
                for (int c = 0; c < outCols; c++) {
                    int parquetCol = projectedColumns == null ? c : projectedColumns[c];
                    // 列裁剪时 Group 索引是投影后的位置(c),不是原列索引(parquetCol)
                    int groupCol = projectedColumns == null ? c : c;
                    if (parquetCol < parquetSchema.getFieldCount()
                            && group.getFieldRepetitionCount(groupCol) > 0) {
                        readValue(vectors.get(c), dst + r, group, groupCol);
                    }
                }
            }
            dst += rows;
        }
        root.setRowCount(dst);
        return root;
    }

    @Override
    public long rowCount(Path part, BufferAllocator allocator) {
        try (ParquetFileReader reader = openReader(part)) {
            return reader.getRecordCount();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 打开 reader,显式用 {@link PlainParquetConfiguration} 而非无参
     * {@code ParquetReadOptions.builder()}:后者内部 {@code new HadoopParquetConfiguration()}
     * 会触发 Hadoop Configuration 静态初始化(引 mapreduce 的 FileInputFormat),把运行时
     * 重新拖回 hadoop-* 依赖。PlainParquetConfiguration 全程 Hadoop-free。
     */
    private static ParquetFileReader openReader(Path part) throws IOException {
        return ParquetFileReader.open(new NioInputFile(part), options());
    }

    private static ParquetReadOptions options() {
        return ParquetReadOptions.builder(new PlainParquetConfiguration()).build();
    }

    private static void writeValue(Group group, int col, int row, FieldVector vector) {
        if (vector instanceof IntVector v) {
            group.add(col, v.get(row));
        } else if (vector instanceof BigIntVector v) {
            group.add(col, v.get(row));
        } else if (vector instanceof SmallIntVector v) {
            group.add(col, (int) v.get(row));
        } else if (vector instanceof Float4Vector v) {
            group.add(col, v.get(row));
        } else if (vector instanceof Float8Vector v) {
            group.add(col, v.get(row));
        } else if (vector instanceof VarCharVector v) {
            group.add(col, new String(v.get(row), StandardCharsets.UTF_8));
        } else if (vector instanceof BitVector v) {
            group.add(col, v.get(row) != 0);
        } else if (vector instanceof DateDayVector v) {
            group.add(col, v.get(row));
        } else if (vector instanceof TimeMilliVector v) {
            group.add(col, v.get(row));
        } else if (vector instanceof TimeStampMilliVector v) {
            group.add(col, v.get(row));
        } else if (vector instanceof VarBinaryVector v) {
            group.add(col, Binary.fromConstantByteArray(v.get(row)));
        } else if (vector instanceof DecimalVector v) {
            writeDecimal(group, col, row, v);
        } else {
            throw new UnsupportedOperationException(
                    "parquet write unsupported type: " + vector.getClass().getSimpleName());
        }
    }

    private static void readValue(FieldVector vector, int row, Group group, int col) {
        if (vector instanceof IntVector v) {
            v.setSafe(row, group.getInteger(col, 0));
        } else if (vector instanceof BigIntVector v) {
            v.setSafe(row, group.getLong(col, 0));
        } else if (vector instanceof SmallIntVector v) {
            v.setSafe(row, (short) group.getInteger(col, 0));
        } else if (vector instanceof Float4Vector v) {
            v.setSafe(row, group.getFloat(col, 0));
        } else if (vector instanceof Float8Vector v) {
            v.setSafe(row, group.getDouble(col, 0));
        } else if (vector instanceof VarCharVector v) {
            // 直接用 getBinary 取 UTF-8 字节,跳过 getString 的
            // Binary→String→byte[] 往返解码(火焰图热点 ~6.8%)
            v.setSafe(row, group.getBinary(col, 0).getBytes());
        } else if (vector instanceof BitVector v) {
            v.setSafe(row, group.getBoolean(col, 0) ? 1 : 0);
        } else if (vector instanceof DateDayVector v) {
            v.setSafe(row, group.getInteger(col, 0));
        } else if (vector instanceof TimeMilliVector v) {
            v.setSafe(row, group.getInteger(col, 0));
        } else if (vector instanceof TimeStampMilliVector v) {
            v.setSafe(row, group.getLong(col, 0));
        } else if (vector instanceof VarBinaryVector v) {
            v.setSafe(row, group.getBinary(col, 0).getBytes());
        } else if (vector instanceof DecimalVector v) {
            readDecimal(v, row, group, col);
        } else {
            throw new UnsupportedOperationException(
                    "parquet read unsupported type: " + vector.getClass().getSimpleName());
        }
    }

    private static void writeDecimal(Group group, int col, int row, DecimalVector v) {
        ArrowType.Decimal dt = (ArrowType.Decimal) v.getField().getType();
        BigDecimal bd = v.getObject(row);
        if (dt.getPrecision() <= 9) {
            group.add(col, bd.unscaledValue().intValue());
        } else if (dt.getPrecision() <= 18) {
            group.add(col, bd.unscaledValue().longValue());
        } else {
            group.add(col, Binary.fromConstantByteArray(bd.unscaledValue().toByteArray()));
        }
    }

    private static void readDecimal(DecimalVector v, int row, Group group, int col) {
        ArrowType.Decimal dt = (ArrowType.Decimal) v.getField().getType();
        int scale = dt.getScale();
        if (dt.getPrecision() <= 9) {
            v.setSafe(row, BigDecimal.valueOf(group.getInteger(col, 0), scale));
        } else if (dt.getPrecision() <= 18) {
            v.setSafe(row, BigDecimal.valueOf(group.getLong(col, 0), scale));
        } else {
            byte[] bytes = group.getBinary(col, 0).getBytes();
            v.setSafe(row, new BigDecimal(new BigInteger(bytes), scale));
        }
    }

    /** 基于 FileChannel 的 {@link InputFile},Hadoop-free。 */
    private static final class NioInputFile implements InputFile {
        private final Path path;

        NioInputFile(Path path) {
            this.path = path;
        }

        @Override
        public long getLength() throws IOException {
            return Files.size(path);
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new NioSeekableInputStream(path);
        }
    }

    /** 基于 FileChannel 的 {@link OutputFile},Hadoop-free。 */
    private static final class NioOutputFile implements OutputFile {
        private final Path path;

        NioOutputFile(Path path) {
            this.path = path;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return createOrOverwrite(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            Files.createDirectories(path.getParent());
            return new NioPositionOutputStream(path);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private static final class NioSeekableInputStream extends SeekableInputStream {
        private final FileChannel channel;

        NioSeekableInputStream(Path path) throws IOException {
            this.channel = FileChannel.open(path, StandardOpenOption.READ);
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
            channel.position(newPos);
        }

        @Override
        public int read() throws IOException {
            ByteBuffer one = ByteBuffer.allocate(1);
            int n = channel.read(one);
            return n < 0 ? -1 : one.get(0) & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(b, off, len);
            return channel.read(buf);
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            return channel.read(buf);
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(b, off, len);
            readFully(buf);
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                if (channel.read(buf) < 0) {
                    throw new EOFException();
                }
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    private static final class NioPositionOutputStream extends PositionOutputStream {
        private final FileChannel channel;

        NioPositionOutputStream(Path path) throws IOException {
            this.channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void write(int b) throws IOException {
            ByteBuffer one = ByteBuffer.allocate(1);
            one.put(0, (byte) b);
            channel.write(one);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(b, off, len);
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }

    // ---- 内存版 InputFile/OutputFile:块级编解码不落盘(SSTable 块读写) ----

    private static final class ByteArrayInputFile implements InputFile {
        private final byte[] data;

        ByteArrayInputFile(byte[] data) {
            this.data = data;
        }

        @Override
        public long getLength() {
            return data.length;
        }

        @Override
        public SeekableInputStream newStream() {
            return new ByteArraySeekableInputStream(data);
        }
    }

    private static final class ByteArraySeekableInputStream extends SeekableInputStream {
        private final byte[] data;
        private int pos;

        ByteArraySeekableInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            this.pos = (int) newPos;
        }

        @Override
        public int read() {
            return pos < data.length ? data[pos++] & 0xFF : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public int read(ByteBuffer buf) {
            int n = Math.min(buf.remaining(), data.length - pos);
            if (n <= 0) {
                return -1;
            }
            buf.put(data, pos, n);
            pos += n;
            return n;
        }

        @Override
        public void readFully(byte[] b) {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) {
            if (pos + len > data.length) {
                throw new IllegalStateException("readFully past end of in-memory data");
            }
            System.arraycopy(data, pos, b, off, len);
            pos += len;
        }

        @Override
        public void readFully(ByteBuffer buf) {
            if (pos + buf.remaining() > data.length) {
                throw new IllegalStateException("readFully past end of in-memory data");
            }
            buf.put(data, pos, buf.remaining());
            pos += buf.remaining();
        }

        @Override
        public void close() {
            // 内存数据,无需释放
        }
    }

    private static final class ByteArrayOutputFile implements OutputFile {
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();

        byte[] toByteArray() {
            return buf.toByteArray();
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) {
            return createOrOverwrite(blockSizeHint);
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) {
            return new ByteArrayPositionOutputStream(buf);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    private static final class ByteArrayPositionOutputStream extends PositionOutputStream {
        private final ByteArrayOutputStream out;

        ByteArrayPositionOutputStream(ByteArrayOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() {
            return out.size();
        }

        @Override
        public void write(int b) {
            out.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) {
            out.write(b, off, len);
        }

        @Override
        public void close() {
            // 不关闭底层缓冲(writeToBytes 调用方在 toByteArray 后统一处理)
        }
    }
}
