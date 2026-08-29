package com.minidb.storage.arrow;

import com.minidb.storage.common.PartFormat;

import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Arrow IPC 文件格式的 part 读写。 */
public class ArrowPartFormat implements PartFormat {

    @Override
    public void write(Path part, VectorSchemaRoot batch) {
        try {
            Files.createDirectories(part.getParent());
            try (SeekableByteChannel channel =
                            Files.newByteChannel(
                                    part,
                                    StandardOpenOption.CREATE,
                                    StandardOpenOption.WRITE,
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

    @Override
    public VectorSchemaRoot read(Path part, Schema schema, BufferAllocator allocator) {
        return read(part, schema, allocator, null);
    }

    @Override
    public VectorSchemaRoot read(
            Path part, Schema schema, BufferAllocator allocator, int[] projectedColumns) {
        try (SeekableByteChannel channel = Files.newByteChannel(part, StandardOpenOption.READ);
                ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            // 列裁剪:只分配投影列的输出向量
            Schema projSchema;
            int[] cols;
            if (projectedColumns == null || projectedColumns.length == schema.getFields().size()) {
                projSchema = schema;
                cols = null;
            } else {
                List<Field> projFields = new ArrayList<>();
                for (int col : projectedColumns) {
                    projFields.add(schema.getFields().get(col));
                }
                projSchema = new Schema(projFields, schema.getCustomMetadata());
                cols = projectedColumns;
            }
            VectorSchemaRoot out = VectorSchemaRoot.create(projSchema, allocator);
            out.allocateNew();
            int dst = 0;
            int outCols = cols == null ? src.getFieldVectors().size() : cols.length;
            while (reader.loadNextBatch()) {
                int batchRows = src.getRowCount();
                for (int i = 0; i < batchRows; i++) {
                    for (int c = 0; c < outCols; c++) {
                        int srcCol = cols == null ? c : cols[c];
                        out.getVector(c).copyFromSafe(i, dst + i, src.getVector(srcCol));
                    }
                }
                dst += batchRows;
            }
            out.setRowCount(dst);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte[] writeToBytes(VectorSchemaRoot batch) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowFileWriter writer =
                    new ArrowFileWriter(batch, null, Channels.newChannel(out))) {
                writer.start();
                writer.writeBatch();
                writer.end();
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public VectorSchemaRoot read(byte[] data, Schema schema, BufferAllocator allocator) {
        try (SeekableByteChannel channel = new ByteArrayReadableSeekableByteChannel(data);
                ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            VectorSchemaRoot out = VectorSchemaRoot.create(schema, allocator);
            out.allocateNew();
            int dst = 0;
            while (reader.loadNextBatch()) {
                int batchRows = src.getRowCount();
                for (int i = 0; i < batchRows; i++) {
                    for (int c = 0; c < src.getFieldVectors().size(); c++) {
                        out.getVector(c).copyFromSafe(i, dst + i, src.getVector(c));
                    }
                }
                dst += batchRows;
            }
            out.setRowCount(dst);
            return out;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long rowCount(Path part, BufferAllocator allocator) {
        try (SeekableByteChannel channel = Files.newByteChannel(part, StandardOpenOption.READ);
                ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            long count = 0;
            for (ArrowBlock block : reader.getRecordBlocks()) {
                count += recordBatchLength(channel, block);
            }
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 只读一个 record batch 的 metadata(flatbuffer)取行数,不读 body。 */
    private static long recordBatchLength(SeekableByteChannel channel, ArrowBlock block)
            throws IOException {
        channel.position(block.getOffset());
        ByteBuffer buf = ByteBuffer.allocate(block.getMetadataLength());
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) {
                throw new EOFException();
            }
        }
        buf.flip();
        int prefixSize =
                buf.remaining() >= 4 && buf.getInt(0) == MessageSerializer.IPC_CONTINUATION_TOKEN
                        ? 8
                        : 4;
        buf.position(prefixSize);
        Message message = Message.getRootAsMessage(buf);
        RecordBatch recordBatch = (RecordBatch) message.header(new RecordBatch());
        return recordBatch.length();
    }
}
