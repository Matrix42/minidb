package com.minidb.storage.lsm;

import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.RowValue;
import com.minidb.storage.common.TableSchema;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class WAL implements AutoCloseable {

    public record Entry(List<Object> key, RowValue value) {}

    private final FileChannel channel;
    private final TableSchema schema;
    private final CRC32 crc = new CRC32();
    private final ByteBuffer writeBuf = ByteBuffer.allocate(8192);

    public WAL(Path walFile, TableSchema schema) throws IOException {
        this.schema = schema;
        this.channel = FileChannel.open(walFile,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.channel.position(this.channel.size()); // 追加写
    }

    public void append(List<Object> key, RowValue value) {
        try {
            byte[] entryBytes = encodeEntry(key, value);
            crc.reset();
            crc.update(entryBytes);
            int checksum = (int) crc.getValue();

            writeBuf.clear();
            writeBuf.putInt(checksum);
            writeBuf.putInt(entryBytes.length);
            writeBuf.put(entryBytes);
            writeBuf.flip();
            while (writeBuf.hasRemaining()) {
                channel.write(writeBuf);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<Entry> recover() {
        List<Entry> entries = new ArrayList<>();
        try {
            if (channel.size() == 0) {
                return entries;
            }
            channel.position(0);
            ByteBuffer header = ByteBuffer.allocate(8);
            while (channel.position() < channel.size()) {
                header.clear();
                readFully(header);
                header.flip();
                int checksum = header.getInt();
                int length = header.getInt();
                if (length <= 0 || length > 10 * 1024 * 1024) {
                    break; // 损坏或异常长度，停止恢复
                }
                ByteBuffer body = ByteBuffer.allocate(length);
                readFully(body);
                crc.reset();
                crc.update(body.array());
                if ((int) crc.getValue() != checksum) {
                    break; // checksum 不匹配，停止恢复
                }
                entries.add(decodeEntry(body.array()));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return entries;
    }

    public void truncate() {
        try {
            channel.truncate(0);
            channel.position(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void readFully(ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) {
                throw new EOFException();
            }
        }
    }

    private byte[] encodeEntry(List<Object> key, RowValue value) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeByte(value.kind());
        // Key
        dos.writeShort(key.size());
        for (Object k : key) {
            byte[] kBytes = encodeKeyValue(k);
            dos.writeInt(kBytes.length);
            dos.write(kBytes);
        }
        // Values
        if (value.values() == null) {
            dos.writeShort(0);
        } else {
            dos.writeShort(value.values().length);
            for (Object v : value.values()) {
                byte[] vBytes = encodeKeyValue(v);
                dos.writeInt(vBytes.length);
                dos.write(vBytes);
            }
        }
        dos.flush();
        return bos.toByteArray();
    }

    private Entry decodeEntry(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        byte kind = buf.get();
        int keyLen = Short.toUnsignedInt(buf.getShort());
        List<Object> key = new ArrayList<>(keyLen);
        for (int i = 0; i < keyLen; i++) {
            int len = buf.getInt();
            byte[] kBytes = new byte[len];
            buf.get(kBytes);
            key.add(decodeKeyValue(kBytes));
        }
        int valLen = Short.toUnsignedInt(buf.getShort());
        Object[] values = valLen == 0 ? null : new Object[valLen];
        if (valLen > 0) {
            for (int i = 0; i < valLen; i++) {
                int len = buf.getInt();
                byte[] vBytes = new byte[len];
                buf.get(vBytes);
                values[i] = decodeKeyValue(vBytes);
            }
        }
        return new Entry(key, new RowValue(kind, values));
    }

    private byte[] encodeKeyValue(Object obj) {
        if (obj == null) return new byte[0];
        // 用字符串中间格式（与 MemTable 的 key 一致）
        String s = obj.toString();
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private Object decodeKeyValue(byte[] bytes) {
        // WAL 恢复时所有值都是字符串，后续由 LSMTable 按 schema 类型转换
        return new String(bytes, StandardCharsets.UTF_8);
    }
}