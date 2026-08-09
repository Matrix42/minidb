package com.minidb.jdbc;

import com.minidb.server.MiniDbServer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PersistenceTest {

    @Test
    void dataSurvivesRestart() throws Exception {
        Path dataDir = Files.createTempDirectory("minidb-persist");

        MiniDbServer server = new MiniDbServer();
        server.start(0, dataDir);
        String url = "jdbc:minidb://127.0.0.1:" + server.port();
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.execute("CREATE TABLE keep (id INTEGER, name VARCHAR)");
            s.executeUpdate("INSERT INTO keep VALUES (1, 'x')");
        }
        server.close();

        assertTrue(Files.exists(dataDir.resolve("keep.arrow")));

        MiniDbServer server2 = new MiniDbServer();
        server2.start(0, dataDir);
        String url2 = "jdbc:minidb://127.0.0.1:" + server2.port();
        try (Connection c = DriverManager.getConnection(url2);
             Statement s = c.createStatement();
             ResultSet rs = s.executeQuery("SELECT id, name FROM keep")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            assertEquals("x", rs.getString(2));
        }
        server2.close();
    }
}
