package com.minidb.server.transaction;

import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.CRC32;

/**
 * 全局事务日志，记录 COMMIT/ABORT 决定。 格式：[checksum:4][length:4][txId:8][status:1] status: 0=COMMIT, 1=ABORT
 * 只追加，每条记录 fsync。
 */
public class TxLog implements AutoCloseable {

    public static final byte STATUS_COMMIT = 0;
    public static final byte STATUS_ABORT = 1;

    private static final int PAYLOAD_SIZE = 9; // txId(8) + status(1)
    private static final int HEADER_SIZE = 8; // checksum(4) + length(4)

    private final FileChannel channel;

    public TxLog(Path path) {
        try {
            Files.createDirectories(path.getParent());
            this.channel =
                    FileChannel.open(
                            path,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
            this.channel.position(this.channel.size());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 追加一条事务决定记录并 fsync。
     *
     * @param txId 事务 ID
     * @param status STATUS_COMMIT 或 STATUS_ABORT
     */
    public void append(long txId, byte status) {
        try {
            ByteBuffer payload = ByteBuffer.allocate(PAYLOAD_SIZE);
            payload.putLong(txId);
            payload.put(status);
            payload.flip();

            CRC32 crc = new CRC32();
            crc.update(payload.array());
            int checksum = (int) crc.getValue();

            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            header.putInt(checksum);
            header.putInt(PAYLOAD_SIZE);
            header.flip();

            channel.write(header);
            channel.write(payload);
            channel.force(true); // fsync: 保证 COMMIT 决定持久化
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 恢复：读取所有 COMMITTED 的事务 ID。
     *
     * @return 已提交事务的 txId 集合
     */
    public Set<Long> recoverCommitted() {
        Set<Long> committed = new HashSet<>();
        try {
            if (channel.size() == 0) {
                return committed;
            }
            long savedPos = channel.position();
            channel.position(0);
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            while (channel.position() < channel.size()) {
                try {
                    header.clear();
                    readFully(header);
                    header.flip();
                    int checksum = header.getInt();
                    int length = header.getInt();
                    if (length != PAYLOAD_SIZE) {
                        break; // 异常长度，停止
                    }
                    ByteBuffer body = ByteBuffer.allocate(length);
                    readFully(body);
                    CRC32 crc = new CRC32();
                    crc.update(body.array());
                    if ((int) crc.getValue() != checksum) {
                        break; // checksum 不匹配，停止
                    }
                    long txId = body.getLong(0);
                    byte status = body.get(8);
                    if (status == STATUS_COMMIT) {
                        committed.add(txId);
                    }
                } catch (EOFException e) {
                    break; // 文件截断，停止
                }
            }
            channel.position(savedPos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return committed;
    }

    /** 清空日志（所有活跃事务已结束）。 */
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
}
