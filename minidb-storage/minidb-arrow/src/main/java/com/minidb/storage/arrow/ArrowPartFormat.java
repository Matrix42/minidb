package com.minidb.storage.arrow;

import com.minidb.storage.common.PartFormat;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.MessageSerializer;

/** Arrow IPC 文件格式的 part 读写。 */
public class ArrowPartFormat implements PartFormat {

    @Override
    public void write(Path part, VectorSchemaRoot batch) {
        try {
            Files.createDirectories(part.getParent());
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

    @Override
    public VectorSchemaRoot read(Path part, org.apache.arrow.vector.types.pojo.Schema schema,
                                 BufferAllocator allocator) {
        try (SeekableByteChannel channel = Files.newByteChannel(part, StandardOpenOption.READ);
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
        ByteBuffer buf = ByteBuffer.allocate((int) block.getMetadataLength());
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) {
                throw new EOFException();
            }
        }
        buf.flip();
        int prefixSize = buf.remaining() >= 4
                && buf.getInt(0) == MessageSerializer.IPC_CONTINUATION_TOKEN ? 8 : 4;
        buf.position(prefixSize);
        Message message = Message.getRootAsMessage(buf);
        RecordBatch recordBatch = (RecordBatch) message.header(new RecordBatch());
        return recordBatch.length();
    }
}
