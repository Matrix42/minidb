package com.minidb.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.minidb.server.MiniDbServer;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class ResultSetPaginationTest {

    static MiniDbServer server;
    static String url;

    @BeforeAll
    static void startServer() throws Exception {
        server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-paging"));
        url = "jdbc:minidb://127.0.0.1:" + server.port();
    }

    @AfterAll
    static void stopServer() {
        server.close();
    }

    @Test
    void setFetchSizePaginatesAcrossPages() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE p (id INTEGER)");
            s.executeUpdate("INSERT INTO p VALUES (1), (2), (3), (4), (5)");
            s.setFetchSize(2);
            try (ResultSet rs = s.executeQuery("SELECT id FROM p ORDER BY id")) {
                int count = 0;
                int sum = 0;
                while (rs.next()) {
                    count++;
                    sum += rs.getInt(1);
                }
                assertEquals(5, count);
                assertEquals(15, sum);
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void defaultFetchSizeReturnsAllRows() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE d (id INTEGER)");
            s.executeUpdate("INSERT INTO d VALUES (1), (2), (3)");
            try (ResultSet rs = s.executeQuery("SELECT id FROM d ORDER BY id")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(3, count);
            }
        }
    }

    @Test
    void metadataStableAcrossPages() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE m (id INTEGER)");
            s.executeUpdate("INSERT INTO m VALUES (1), (2), (3)");
            s.setFetchSize(2);
            try (ResultSet rs = s.executeQuery("SELECT id, id * 2 AS doubled FROM m ORDER BY id")) {
                assertEquals(2, rs.getMetaData().getColumnCount());
                assertEquals("id", rs.getMetaData().getColumnName(1));
                assertEquals("doubled", rs.getMetaData().getColumnName(2));
                while (rs.next()) {
                    // drive through all pages; metadata must remain readable
                }
                assertEquals(2, rs.getMetaData().getColumnCount());
            }
        }
    }

    @Test
    void earlyCloseDoesNotThrow() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE c (id INTEGER)");
            s.executeUpdate("INSERT INTO c VALUES (1), (2), (3), (4)");
            s.setFetchSize(2);
            try (ResultSet rs = s.executeQuery("SELECT id FROM c ORDER BY id")) {
                assertTrue(rs.next());
                assertTrue(rs.next());
                // closes early, before exhausting the cursor
            }
            // server must have released the cursor; no assertion beyond no-throw
        }
    }

    @Test
    void largeResultStreamsInPages() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE big (id INTEGER)");
            StringBuilder insert = new StringBuilder("INSERT INTO big VALUES ");
            for (int i = 1; i <= 4100; i++) {
                if (i > 1) {
                    insert.append(',');
                }
                insert.append('(').append(i).append(')');
            }
            s.executeUpdate(insert.toString());
            // default fetchSize (4096) → two pages for 4100 rows
            try (ResultSet rs = s.executeQuery("SELECT id FROM big ORDER BY id")) {
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                assertEquals(4100, count);
            }
        }
    }
}
