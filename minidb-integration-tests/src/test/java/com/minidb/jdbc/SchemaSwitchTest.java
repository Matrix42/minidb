package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** JDBC schema 切换(bug #20):setSchema/getSchema 对接服务端 USE SCHEMA。 */
class SchemaSwitchTest {

    @Test
    void setSchemaSwitchesConnectionCurrentSchema() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-schema");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
                Statement s = c.createStatement()) {
            assertEquals("public", c.getSchema(), "默认 schema 应为 public");

            s.execute("CREATE SCHEMA other");
            s.execute("CREATE TABLE other.t (id INTEGER)");
            s.execute("INSERT INTO other.t VALUES (1)");

            c.setSchema("other");
            assertEquals("other", c.getSchema());

            // 无 schema 限定的建表/查询现在落在 other
            s.execute("CREATE TABLE u (id INTEGER)");
            try (java.sql.ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM t")) {
                rs.next();
                assertEquals(1, rs.getInt(1), "切到 other 后裸表名应解析到 other");
            }
        } finally {
            server.close();
        }
    }

    @Test
    void setSchemaToMissingSchemaThrows() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-schema-miss");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url)) {
            assertThrows(java.sql.SQLException.class, () -> c.setSchema("ghost"));
            assertEquals("public", c.getSchema(), "切换失败应保持原 schema");
        } finally {
            server.close();
        }
    }
}
