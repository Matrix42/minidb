package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlterTableTest {

    @Test
    void alterTableEndToEnd() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-alter");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
            s.executeUpdate("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
            s.execute("ALTER TABLE t ADD extra INTEGER DEFAULT 10");
            s.execute("ALTER TABLE t RENAME COLUMN name TO label");
            s.execute("ALTER TABLE t DROP COLUMN label");
            s.execute("ALTER TABLE t ALTER COLUMN id SET DATA TYPE BIGINT");
            try (ResultSet rs = s.executeQuery("SELECT id, extra FROM t ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1L, rs.getLong(1));
                assertEquals(10, rs.getInt(2));
                assertTrue(rs.next());
                assertEquals(2L, rs.getLong(1));
                assertEquals(10, rs.getInt(2));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void alterTableSurvivesRestart() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-alter-persist");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (id INTEGER)");
            s.executeUpdate("INSERT INTO t VALUES (1)");
            s.execute("ALTER TABLE t ADD name VARCHAR DEFAULT 'x'");
            s.execute("ALTER TABLE t RENAME TO t2");
        } finally {
            server.close();
        }

        MiniDbServer server2 = new MiniDbServer();
        server2.start(0, dataDir);
        String url2 = "jdbc:minidb://127.0.0.1:" + server2.port();
        try (Connection c = DriverManager.getConnection(url2);
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT id, name FROM t2")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("x", rs.getString(2));
            assertFalse(rs.next());
        } finally {
            server2.close();
        }
    }
}
