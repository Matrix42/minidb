package com.minidb.storage.arrow;

import com.minidb.storage.common.PartFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;

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
    public VectorSchemaRoot read(Path part, BufferAllocator allocator) {
        try (SeekableByteChannel channel = Files.newByteChannel(part, StandardOpenOption.READ);
             ArrowFileReader reader = new ArrowFileReader(channel, allocator)) {
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            VectorSchemaRoot out = VectorSchemaRoot.create(src.getSchema(), allocator);
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
            while (reader.loadNextBatch()) {
                count += reader.getVectorSchemaRoot().getRowCount();
            }
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
