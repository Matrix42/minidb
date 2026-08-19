package com.minidb.storage.lsm;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class BloomFilter {
    private static final double LN2 = 0.6931471805599453;
    private final BitSet bits;
    private final int numHashes;
    private final int bitSize;

    public BloomFilter(int bitsPerKey, int expectedKeys) {
        this.bitSize = Math.max(64, bitsPerKey * expectedKeys);
        this.numHashes = Math.max(1, (int) (bitsPerKey * LN2));
        this.bits = new BitSet(bitSize);
    }

    private BloomFilter(int bitSize, int numHashes, BitSet bits) {
        this.bitSize = bitSize;
        this.numHashes = numHashes;
        this.bits = bits;
    }

    public void add(byte[] key) {
        long h = hash(key);
        for (int i = 0; i < numHashes; i++) {
            int idx = index(h, i);
            bits.set(idx);
        }
    }

    public boolean mightContain(byte[] key) {
        long h = hash(key);
        for (int i = 0; i < numHashes; i++) {
            if (!bits.get(index(h, i))) {
                return false;
            }
        }
        return true;
    }

    /** 使用 MurmurHash3 风格的 64-bit hash，模拟多个 hash 函数 */
    private static long hash(byte[] key) {
        long h = 0xcbf29ce484222325L;
        for (byte b : key) {
            h ^= (b & 0xFF);
            h *= 0x100000001b3L;
        }
        return h;
    }

    private int index(long h, int i) {
        // 双 hash 技巧: h1 + i*h2 模拟多个独立 hash 函数
        long combined = h + (long) i * Long.rotateLeft(h, 17);
        return Math.abs((int) (combined % bitSize));
    }

    public byte[] toBytes() {
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + (bitSize + 7) / 8);
        buf.putInt(bitSize);
        buf.putInt(numHashes);
        buf.put(bits.toByteArray());
        return buf.array();
    }

    public static BloomFilter fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int bitSize = buf.getInt();
        int numHashes = buf.getInt();
        byte[] bitBytes = new byte[bytes.length - 8];
        buf.get(bitBytes);
        return new BloomFilter(bitSize, numHashes, BitSet.valueOf(bitBytes));
    }
}