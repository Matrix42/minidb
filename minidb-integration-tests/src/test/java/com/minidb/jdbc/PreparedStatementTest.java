package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PreparedStatementTest {

    @Test
    void setByteShortFloatRoundTrip() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-ps");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (a SMALLINT, b BIGINT, c DOUBLE)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES (?, ?, ?)")) {
                ps.setByte(1, (byte) 42);
                ps.setShort(2, (short) 300);
                ps.setFloat(3, 3.14f);
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("SELECT a, b, c FROM t")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertEquals(300L, rs.getLong(2));
                assertEquals(3.14f, rs.getDouble(3), 0.001);
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void setTimeRoundTrip() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-ps");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (tm TIME)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES (?)")) {
                ps.setTime(1, Time.valueOf("10:30:00"));
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("SELECT tm FROM t")) {
                assertTrue(rs.next());
                assertEquals(java.time.LocalTime.of(10, 30, 0), rs.getTime(1).toLocalTime());
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void setBytesRoundTrip() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-ps");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (b VARBINARY)");
            byte[] data = {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES (?)")) {
                ps.setBytes(1, data);
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("SELECT b FROM t")) {
                assertTrue(rs.next());
                assertArrayEquals(data, rs.getBytes(1));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void setDateAndTimestampRoundTrip() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-ps");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (d DATE, ts TIMESTAMP)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                ps.setDate(1, Date.valueOf("2025-01-15"));
                ps.setTimestamp(2, Timestamp.valueOf("2025-06-30 14:00:00"));
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("SELECT d, ts FROM t")) {
                assertTrue(rs.next());
                assertEquals("2025-01-15", rs.getDate(1).toString());
                assertEquals("2025-06-30 14:00:00", rs.getTimestamp(2).toString().substring(0, 19));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void setNullRoundTrip() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-ps");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES (?, ?)")) {
                ps.setNull(1, java.sql.Types.INTEGER);
                ps.setString(2, "hello");
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("SELECT id, name FROM t")) {
                assertTrue(rs.next());
                rs.getInt(1);
                assertTrue(rs.wasNull());
                assertEquals("hello", rs.getString(2));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void setObjectMixedTypes() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-ps");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE t (a INTEGER, b DOUBLE, c BOOLEAN, d VARCHAR)");
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO t VALUES (?, ?, ?, ?)")) {
                ps.setObject(1, 100);
                ps.setObject(2, 2.718);
                ps.setObject(3, true);
                ps.setObject(4, "text");
                ps.executeUpdate();
            }
            try (ResultSet rs = s.executeQuery("SELECT a, b, c, d FROM t")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
                assertEquals(2.718, rs.getDouble(2), 0.001);
                assertTrue(rs.getBoolean(3));
                assertEquals("text", rs.getString(4));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }
}
