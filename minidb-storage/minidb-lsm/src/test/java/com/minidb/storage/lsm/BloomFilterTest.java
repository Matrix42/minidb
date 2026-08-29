package com.minidb.storage.lsm;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class BloomFilterTest {
    @Test
    void containsAdded() {
        BloomFilter bf = new BloomFilter(10, 100);
        bf.add(bytes("key1"));
        bf.add(bytes("key2"));
        assertTrue(bf.mightContain(bytes("key1")));
        assertTrue(bf.mightContain(bytes("key2")));
    }

    @Test
    void doesNotContainNotAdded() {
        BloomFilter bf = new BloomFilter(10, 100);
        bf.add(bytes("key1"));
        assertFalse(bf.mightContain(bytes("key2")));
    }

    @Test
    void roundTrip() {
        BloomFilter bf = new BloomFilter(10, 100);
        bf.add(bytes("a"));
        bf.add(bytes("b"));
        bf.add(bytes("c"));
        byte[] serialized = bf.toBytes();
        BloomFilter bf2 = BloomFilter.fromBytes(serialized);
        assertTrue(bf2.mightContain(bytes("a")));
        assertTrue(bf2.mightContain(bytes("b")));
        assertTrue(bf2.mightContain(bytes("c")));
        assertFalse(bf2.mightContain(bytes("d")));
    }

    @Test
    void emptyFilter() {
        BloomFilter bf = new BloomFilter(10, 100);
        assertFalse(bf.mightContain(bytes("anything")));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
