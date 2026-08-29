package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseMetaDataTest {

    @Test
    void getSchemasListsPublicAndCustomSchema() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE SCHEMA other");
            DatabaseMetaData md = c.getMetaData();
            Set<String> schemas = new HashSet<>();
            try (ResultSet rs = md.getSchemas()) {
                while (rs.next()) {
                    schemas.add(rs.getString("TABLE_SCHEM"));
                }
            }
            assertTrue(schemas.contains("public"));
            assertTrue(schemas.contains("other"));
        } finally {
            server.close();
        }
    }

    @Test
    void getTablesAndColumnsRoundTrip() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE public.users (id INTEGER, name VARCHAR)");
            s.execute("CREATE SCHEMA other");
            s.execute("CREATE TABLE other.t (a BIGINT, b BOOLEAN)");
            DatabaseMetaData md = c.getMetaData();

            Set<String> tables = new HashSet<>();
            try (ResultSet rs = md.getTables(null, null, null, null)) {
                while (rs.next()) {
                    tables.add(rs.getString("TABLE_SCHEM") + "." + rs.getString("TABLE_NAME"));
                    // 系统表为 SYSTEM TABLE,用户表为 TABLE
                    String type = rs.getString("TABLE_TYPE");
                    assertTrue(
                            "TABLE".equals(type) || "SYSTEM TABLE".equals(type),
                            "unexpected TABLE_TYPE: " + type);
                }
            }
            assertTrue(tables.contains("public.users"));
            assertTrue(tables.contains("other.t"));

            try (ResultSet rs = md.getTables(null, "public", null, null)) {
                assertTrue(rs.next());
                assertEquals("users", rs.getString("TABLE_NAME"));
                assertFalse(rs.next());
            }

            try (ResultSet rs = md.getTables(null, null, null, new String[] {"VIEW"})) {
                assertFalse(rs.next());
                // empty result set still carries full getTables schema over the wire
                assertEquals(10, rs.getMetaData().getColumnCount());
            }

            try (ResultSet rs = md.getColumns(null, null, "users", null)) {
                assertTrue(rs.next());
                assertEquals("id", rs.getString("COLUMN_NAME"));
                assertEquals("INTEGER", rs.getString("TYPE_NAME"));
                assertEquals(java.sql.Types.INTEGER, rs.getInt("DATA_TYPE"));
                assertEquals(1, rs.getInt("ORDINAL_POSITION"));
                assertEquals(1, rs.getInt("NULLABLE"));
                assertEquals("YES", rs.getString("IS_NULLABLE"));
                assertEquals("NO", rs.getString("IS_AUTOINCREMENT"));
                assertEquals(10, rs.getInt("NUM_PREC_RADIX"));
                assertTrue(rs.next());
                assertEquals("name", rs.getString("COLUMN_NAME"));
                assertEquals(2, rs.getInt("ORDINAL_POSITION"));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void getTableTypesListsTable() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url)) {
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getTableTypes()) {
                Set<String> types = new HashSet<>();
                while (rs.next()) {
                    types.add(rs.getString("TABLE_TYPE"));
                }
                assertEquals(3, types.size());
                assertTrue(types.contains("TABLE"));
                assertTrue(types.contains("VIEW"));
                assertTrue(types.contains("SYSTEM TABLE"));
            }
        } finally {
            server.close();
        }
    }

    @Test
    void getColumnsFilterByLikeColumnName() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            s.execute("CREATE TABLE public.users (id INTEGER, username VARCHAR)");
            DatabaseMetaData md = c.getMetaData();
            try (ResultSet rs = md.getColumns(null, null, null, "%name%")) {
                assertTrue(rs.next());
                assertEquals("username", rs.getString("COLUMN_NAME"));
                assertFalse(rs.next());
            }
        } finally {
            server.close();
        }
    }

    @Test
    void supportsFlagsReportRealCapabilities() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-meta");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            DatabaseMetaData md = c.getMetaData();
            // 实际支持的能力必须报 true,否则 DataGrip/DBeaver 会据此禁用功能
            assertTrue(md.supportsGroupBy());
            assertTrue(md.supportsOuterJoins());
            assertTrue(md.supportsFullOuterJoins());
            assertTrue(md.supportsUnion());
            assertTrue(md.supportsUnionAll());
            assertTrue(md.supportsSubqueriesInComparisons());
            assertTrue(md.supportsSubqueriesInExists());
            assertTrue(md.supportsSubqueriesInIns());
            assertTrue(md.supportsSchemasInDataManipulation());
            assertTrue(md.supportsIntegrityEnhancementFacility());
            // LIKE ESCAPE 与 getSearchStringEscape() 一致
            assertTrue(md.supportsLikeEscapeClause());
            assertEquals("\\", md.getSearchStringEscape());
            // 多表 JOIN 无上限(0 = 无限制),不得误报为 1
            assertEquals(0, md.getMaxTablesInSelect());
        } finally {
            server.close();
        }
    }
}
