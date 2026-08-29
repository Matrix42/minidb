package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 事务生命周期端到端:setAutoCommit(false) 隐式 BEGIN,commit 持久化、rollback 丢弃。 每个场景用独立连接,避免依赖「commit 后再自动
 * BEGIN」这一未实现语义。
 */
class TransactionLifecycleTest {

    private MiniDbServer startServer() throws Exception {
        MiniDbServer server = new MiniDbServer();
        server.start(0, Files.createTempDirectory("minidb-tx"));
        return server;
    }

    private static int count(Statement s, String sql) throws Exception {
        try (ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    void commitPersists() throws Exception {
        MiniDbServer server = startServer();
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try {
            Connection c = DriverManager.getConnection(url);
            Connection observer = DriverManager.getConnection(url);
            try (Statement s = c.createStatement();
                    Statement o = observer.createStatement()) {
                s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");

                c.setAutoCommit(false);
                s.executeUpdate("INSERT INTO t VALUES (1)");
                // 未提交时其他连接不可见
                assertEquals(0, count(o, "SELECT COUNT(*) FROM t"), "未提交应不可见");
                c.commit();
                assertEquals(1, count(o, "SELECT COUNT(*) FROM t"), "commit 后应持久化");
            }
            c.close();
            observer.close();
        } finally {
            server.close();
        }
    }

    @Test
    void rollbackDiscards() throws Exception {
        MiniDbServer server = startServer();
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try {
            Connection c = DriverManager.getConnection(url);
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
                s.execute("INSERT INTO t VALUES (1)");

                c.setAutoCommit(false);
                s.executeUpdate("INSERT INTO t VALUES (2)");
                c.rollback();
                c.setAutoCommit(true);
                assertEquals(1, count(s, "SELECT COUNT(*) FROM t"), "rollback 后应丢弃");
            }
            c.close();
        } finally {
            server.close();
        }
    }

    @Test
    void failedStatementThenRollback() throws Exception {
        MiniDbServer server = startServer();
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try {
            Connection c = DriverManager.getConnection(url);
            try (Statement s = c.createStatement()) {
                s.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)");
                s.execute("INSERT INTO t VALUES (1)");

                c.setAutoCommit(false);
                assertThrows(
                        java.sql.SQLException.class,
                        () -> s.executeUpdate("INSERT INTO t VALUES (1)"),
                        "主键重复插入应失败");
                // 失败后仍可 rollback,不影响已提交数据
                c.rollback();
                c.setAutoCommit(true);
                assertEquals(1, count(s, "SELECT COUNT(*) FROM t"), "失败的事务不影响已提交数据");
            }
            c.close();
        } finally {
            server.close();
        }
    }
}
