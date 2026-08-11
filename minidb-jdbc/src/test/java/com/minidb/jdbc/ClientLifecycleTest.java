package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

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
        SQLException ex = assertThrows(SQLException.class,
                () -> s.execute("CREATE TABLE gone (id INTEGER)"));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        // Fast-fail must beat the 30s timeout by a wide margin.
        assertTrue(elapsedMs < 5_000,
                "expected fast-fail < 5000ms, got " + elapsedMs);
        assertTrue(ex.getMessage() != null && !ex.getMessage().contains("timeout"),
                "should report connection closed, not timeout: " + ex.getMessage());

        c.close(); // client close must be safe even on a dead connection
    }
}
