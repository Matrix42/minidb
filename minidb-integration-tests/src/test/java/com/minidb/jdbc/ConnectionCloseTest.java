package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** 关闭 Connection 必须连带关闭其创建的 Statement 与 ResultSet(JDBC 规范)。 */
class ConnectionCloseTest {

    @Test
    void closeConnectionClosesStatementsAndResultSets() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-connclose");
        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try {
            Connection c = DriverManager.getConnection(url);
            Statement s = c.createStatement();
            s.execute("CREATE TABLE t (id INTEGER)");
            s.execute("INSERT INTO t VALUES (1), (2)");
            ResultSet rs = s.executeQuery("SELECT id FROM t");

            c.close();

            assertTrue(s.isClosed(), "connection.close 应关闭其创建的 statement");
            assertTrue(rs.isClosed(), "connection.close 应关闭 statement 持有的 result set");
        } finally {
            server.close();
        }
    }
}
