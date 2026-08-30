package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientLifecycleTest {

    @Test
    void restartFailsOpenExecuteFast() throws Exception {
        MiniDbServer server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-restart"));
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        Connection c = DriverManager.getConnection(url);
        Statement s = c.createStatement();

        server.close(); // kill the server; the socket goes away

        long start = System.nanoTime();
        SQLException ex =
                assertThrows(SQLException.class, () -> s.execute("CREATE TABLE gone (id INTEGER)"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // Fast-fail must beat the 30s timeout by a wide margin.
        assertTrue(elapsedMs < 5_000, "expected fast-fail < 5000ms, got " + elapsedMs);
        assertTrue(
                ex.getMessage() != null && !ex.getMessage().contains("timeout"),
                "should report connection closed, not timeout: " + ex.getMessage());

        c.close(); // client close must be safe even on a dead connection
    }

    @Test
    void deadConnectionReportsClosed() throws Exception {
        MiniDbServer server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-dead"));
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        Connection c = DriverManager.getConnection(url);

        // Sanity: while alive, the connection is open and valid.
        assertFalse(c.isClosed(), "should be open before disconnect");
        assertTrue(c.isValid(1), "should be valid before disconnect");

        server.close(); // kill the server; the socket goes away

        // Give the client's channelInactive a moment to fire so connected=false.
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (c.isValid(1) && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }

        // After the network is gone, the JDBC Connection must report itself
        // as closed/invalid so a pool does not hand it back out.
        assertTrue(c.isClosed(), "should report closed after disconnect");
        assertFalse(c.isValid(1), "should report invalid after disconnect");

        c.close();
    }

    @Test
    void concurrentStatementsDoNotCrossTalk() throws Exception {
        MiniDbServer server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-xtalk"));
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        Connection c = DriverManager.getConnection(url);
        try (Statement s = c.createStatement()) {
            s.execute("CREATE TABLE a (id INTEGER)");
            s.executeUpdate("INSERT INTO a VALUES (1), (2), (3)");
            s.execute("CREATE TABLE b (id INTEGER)");
            s.executeUpdate("INSERT INTO b VALUES (10), (20)");
        }

        int threads = 8;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger errors = new AtomicInteger();
        List<Integer> aSums = Collections.synchronizedList(new ArrayList<>());
        List<Integer> bSums = Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threads; i++) {
            int which = i % 2;
            Thread t =
                    new Thread(
                            () -> {
                                try (Statement s = c.createStatement();
                                        java.sql.ResultSet rs =
                                                s.executeQuery(
                                                        which == 0
                                                                ? "SELECT id FROM a ORDER BY id"
                                                                : "SELECT id FROM b ORDER BY id")) {
                                    start.await();
                                    int sum = 0;
                                    while (rs.next()) {
                                        sum += rs.getInt(1);
                                    }
                                    (which == 0 ? aSums : bSums).add(sum);
                                } catch (Exception e) {
                                    errors.incrementAndGet();
                                } finally {
                                    done.countDown();
                                }
                            });
            t.start();
        }

        start.countDown();
        assertTrue(done.await(60, TimeUnit.SECONDS), "threads did not finish");
        assertEquals(0, errors.get(), "some threads threw");

        // Each a-thread must see rows summing to 1+2+3=6; each b-thread 10+20=30.
        assertEquals(threads / 2, aSums.size(), "a-thread count");
        assertEquals(threads / 2, bSums.size(), "b-thread count");
        for (Integer v : aSums) {
            assertEquals(6, v, "a-thread got wrong rows (cross-talk?)");
        }
        for (Integer v : bSums) {
            assertEquals(30, v, "b-thread got wrong rows (cross-talk?)");
        }

        c.close();
        server.close();
    }
}
