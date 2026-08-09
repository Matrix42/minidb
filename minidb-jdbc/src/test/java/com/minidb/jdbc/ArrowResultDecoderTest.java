package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrowResultDecoderTest {

    static MiniDbServer server;
    static String url;

    @BeforeAll
    static void startServer() throws Exception {
        server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-test"));
        url = "jdbc:minidb://127.0.0.1:" + server.port();
    }

    @AfterAll
    static void stopServer() {
        server.close();
    }

    @Test
    void driverAcceptsUrl() throws Exception {
        assertTrue(new MiniDbDriver().acceptsURL(url));
        assertFalse(new MiniDbDriver().acceptsURL("jdbc:mysql://x"));
    }

    @Test
    void endToEndSelectDecodesArrow() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
            s.executeUpdate("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
            try (ResultSet rs = s.executeQuery(
                    "SELECT id, name FROM t ORDER BY id")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals("a", rs.getString(2));
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("id"));
                assertEquals("b", rs.getString("name"));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void nullValuesReported() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE n (id INTEGER, name VARCHAR)");
            s.executeUpdate("INSERT INTO n VALUES (1, NULL)");
            try (ResultSet rs = s.executeQuery("SELECT id, name FROM n")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertNull(rs.getString(2));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Test
    void whereOrderLimitWork() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE w (id INTEGER)");
            s.executeUpdate("INSERT INTO w VALUES (5), (3), (8), (1)");
            try (ResultSet rs = s.executeQuery(
                    "SELECT id FROM w WHERE id > 2 ORDER BY id DESC LIMIT 2")) {
                assertTrue(rs.next());
                assertEquals(8, rs.getInt(1));
                assertTrue(rs.next());
                assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    void badSqlThrowsSQLException() throws Exception {
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            assertThrows(SQLException.class,
                    () -> s.executeQuery("SELECT * FROM does_not_exist"));
        }
    }
}
