package com.minidb.storage.lsm;

import com.minidb.storage.common.ColumnMeta;
import com.minidb.storage.common.ColumnType;
import com.minidb.storage.common.RowValue;
import com.minidb.storage.common.TableSchema;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * 写前日志(WAL),按「代」分段:当前段 {@code wal.log} 接收新 append,双缓冲 flush 时
 * {@link #rotate()} 把当前段改成旧段 {@code wal-<gen>.log} 并开新当前段,对应表落盘后
 * {@link #dropSegment(int)} 删旧段。恢复按代升序重放所有段 + 当前段——swap 出的表
 * 在 flush 完成前其 WAL 数据必须保留(否则 crash 丢数据),完成后再删。
 */
public class WAL implements AutoCloseable {

    public record Entry(long txId, List<Object> key, RowValue value) {
        /** 兼容旧格式（无 txId 的条目视为已提交）。 */
        public Entry(List<Object> key, RowValue value) {
            this(0L, key, value);
        }
    }

    private static final String SEGMENT_PREFIX = "wal-";

    private final Path dir;
    private final Path currentPath; // wal.log
    private final CRC32 crc = new CRC32();
    private final ByteBuffer writeBuf = ByteBuffer.allocate(8192);
    private FileChannel channel;
    private int nextSegmentGen; // 下次 rotate 使用的代号

    public WAL(Path walFile, TableSchema schema) throws IOException {
        this.currentPath = walFile;
        this.dir = walFile.getParent();
        this.nextSegmentGen = maxSegmentGen() + 1;
        this.channel = FileChannel.open(walFile,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.channel.position(this.channel.size()); // 追加写
    }

    public void append(List<Object> key, RowValue value) {
        append(0L, key, value);
    }

    public void append(long txId, List<Object> key, RowValue value) {
        try {
            byte[] entryBytes = encodeEntry(txId, key, value);
            crc.reset();
            crc.update(entryBytes);
            int checksum = (int) crc.getValue();

            int totalSize = 8 + entryBytes.length; // checksum(4) + length(4) + data
            ByteBuffer buf;
            if (totalSize <= writeBuf.capacity()) {
                buf = writeBuf;
                buf.clear();
            } else {
                buf = ByteBuffer.allocate(totalSize);
            }
            buf.putInt(checksum);
            buf.putInt(entryBytes.length);
            buf.put(entryBytes);
            buf.flip();
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 双缓冲切换:当前段(数据 = 换出表)关后改名 {@code wal-<gen>.log},开新的空当前段。
     * 返回旧段代号,落盘完成后由调用方 {@link #dropSegment(int)} 删除。
     */
    public int rotate() {
        int gen = nextSegmentGen++;
        try {
            channel.close();
            Files.move(currentPath, dir.resolve(SEGMENT_PREFIX + gen + ".log"));
            channel = FileChannel.open(currentPath,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            channel.position(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return gen;
    }

    /** 删除代号 gen 的旧段(对应表已落盘)。不存在时幂等。 */
    public void dropSegment(int gen) {
        try {
            Files.deleteIfExists(dir.resolve(SEGMENT_PREFIX + gen + ".log"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 清空全部段(TRUNCATE 用):删所有旧段 + 截断当前段。 */
    public void truncateAll() {
        try {
            channel.truncate(0);
            channel.position(0);
            for (Path seg : listSegments()) {
                Files.deleteIfExists(seg);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 恢复:旧段按代号升序(先写的数据先重放) + 当前段。 */
    public List<Entry> recover() {
        return recover(null);
    }

    /**
     * 恢复并过滤未提交事务的条目:只保留 txId==0(非事务) 或 txId 在 committedTxIds 中的条目。
     * committedTxIds 为 null 时不过滤(向后兼容旧 recover())。
     */
    public List<Entry> recover(Set<Long> committedTxIds) {
        List<Entry> entries = new ArrayList<>();
        List<Path> segments = listSegments();
        segments.sort(Comparator.comparingInt(this::segmentGen));
        for (Path seg : segments) {
            entries.addAll(recoverFile(seg));
        }
        entries.addAll(recoverCurrent());
        if (committedTxIds == null) {
            return entries;
        }
        List<Entry> filtered = new ArrayList<>();
        for (Entry e : entries) {
            if (e.txId() == 0 || committedTxIds.contains(e.txId())) {
                filtered.add(e);
            }
        }
        return filtered;
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

    /** 当前段文件从头读(现有恢复逻辑)。 */
    private List<Entry> recoverCurrent() {
        List<Entry> entries = new ArrayList<>();
        try {
            if (channel.size() == 0) {
                return entries;
            }
            channel.position(0);
            ByteBuffer header = ByteBuffer.allocate(8);
            while (channel.position() < channel.size()) {
                try {
                    header.clear();
                    readFully(channel, header);
                    header.flip();
                    int checksum = header.getInt();
                    int length = header.getInt();
                    if (length <= 0 || length > 10 * 1024 * 1024) {
                        break; // 损坏或异常长度，停止恢复
                    }
                    ByteBuffer body = ByteBuffer.allocate(length);
                    readFully(channel, body);
                    crc.reset();
                    crc.update(body.array());
                    if ((int) crc.getValue() != checksum) {
                        break; // checksum 不匹配，停止恢复
                    }
                    entries.add(decodeEntry(body.array()));
                } catch (EOFException e) {
                    break; // 文件截断（崩溃时部分写入），返回已恢复的条目
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return entries;
    }

    /** 单个旧段文件从头读(只读打开,不碰当前段 channel)。 */
    private List<Entry> recoverFile(Path seg) {
        List<Entry> entries = new ArrayList<>();
        try (FileChannel ch = FileChannel.open(seg, StandardOpenOption.READ)) {
            ByteBuffer header = ByteBuffer.allocate(8);
            while (ch.position() < ch.size()) {
                try {
                    header.clear();
                    readFully(ch, header);
                    header.flip();
                    int checksum = header.getInt();
                    int length = header.getInt();
                    if (length <= 0 || length > 10 * 1024 * 1024) {
                        break;
                    }
                    ByteBuffer body = ByteBuffer.allocate(length);
                    readFully(ch, body);
                    crc.reset();
                    crc.update(body.array());
                    if ((int) crc.getValue() != checksum) {
                        break;
                    }
                    entries.add(decodeEntry(body.array()));
                } catch (EOFException e) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return entries;
    }

    private List<Path> listSegments() {
        List<Path> segments = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, SEGMENT_PREFIX + "*.log")) {
            for (Path p : ds) {
                segments.add(p);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return segments;
    }

    private int maxSegmentGen() {
        int max = -1;
        for (Path seg : listSegments()) {
            max = Math.max(max, segmentGen(seg));
        }
        return max;
    }

    private int segmentGen(Path seg) {
        String name = seg.getFileName().toString(); // wal-<n>.log
        String num = name.substring(SEGMENT_PREFIX.length(), name.length() - ".log".length());
        return Integer.parseInt(num);
    }

    private static void readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            if (ch.read(buf) < 0) {
                throw new EOFException();
            }
        }
    }

    private byte[] encodeEntry(long txId, List<Object> key, RowValue value) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeLong(txId);
        dos.writeByte(value.kind());
        // Key:类型自描述二进制(tag + 定长/长度前缀),恢复时直接还原 Integer/Long/String,
        // 消除「toString → UTF-8 → 重新 parse」三趟,且不会把数字字符串 key 误当整数。
        dos.writeShort(key.size());
        for (Object k : key) {
            byte[] kBytes = encodeKeyValue(k);
            dos.writeInt(kBytes.length);
            dos.write(kBytes);
        }
        // Values:字符串中间格式(与旧格式一致,恢复时按列类型转换)
        if (value.values() == null) {
            dos.writeShort(0);
        } else {
            dos.writeShort(value.values().length);
            for (Object v : value.values()) {
                byte[] vBytes = encodeValueString(v);
                dos.writeInt(vBytes.length);
                dos.write(vBytes);
            }
        }
        dos.flush();
        return bos.toByteArray();
    }

    private Entry decodeEntry(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        long txId = buf.getLong();
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
                values[i] = decodeValueString(vBytes);
            }
        }
        return new Entry(txId, key, new RowValue(kind, values));
    }

    /** 单个 key 值编码:类型 tag + 数据,定长整数(4/8/2 字节),字符串长度前缀。 */
    private static byte[] encodeKeyValue(Object k) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(12);
        DataOutputStream dos = new DataOutputStream(bos);
        try {
            if (k == null) {
                dos.writeByte(0);
            } else if (k instanceof Integer i) {
                dos.writeByte(1);
                dos.writeInt(i);
            } else if (k instanceof Long l) {
                dos.writeByte(2);
                dos.writeLong(l);
            } else if (k instanceof Short s) {
                dos.writeByte(3);
                dos.writeShort(s);
            } else {
                dos.writeByte(4);
                byte[] utf8 = k.toString().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(utf8.length);
                dos.write(utf8);
            }
            dos.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return bos.toByteArray();
    }

    /** 单个 key 值解码(encodeKeyValue 的逆):按 tag 还原类型。 */
    private static Object decodeKeyValue(byte[] bytes) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
            return switch (dis.readByte()) {
                case 0 -> null;
                case 1 -> dis.readInt();
                case 2 -> dis.readLong();
                case 3 -> dis.readShort();
                default -> {
                    byte[] utf8 = new byte[dis.readInt()];
                    dis.readFully(utf8);
                    yield new String(utf8, StandardCharsets.UTF_8);
                }
            };
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** 单个 value 编码:字符串中间格式(与旧格式一致)。 */
    private static byte[] encodeValueString(Object v) {
        if (v == null) return new byte[] { 0 }; // sentinel: 0x00 = null
        String s = v.toString();
        byte[] utf8 = s.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[1 + utf8.length];
        result[0] = 1; // sentinel: 0x01 = non-null string
        System.arraycopy(utf8, 0, result, 1, utf8.length);
        return result;
    }

    /** 单个 value 解码(字符串中间格式)。 */
    private static Object decodeValueString(byte[] bytes) {
        if (bytes.length == 0) return ""; // 向后兼容旧格式（无 sentinel 的空字符串）
        if (bytes[0] == 0) return null; // sentinel: null
        // bytes[0] == 1: non-null string; 也兼容旧格式（无 sentinel 前缀）
        int offset = bytes[0] == 1 ? 1 : 0;
        return new String(bytes, offset, bytes.length - offset, StandardCharsets.UTF_8);
    }
}
