package com.minidb.jdbc;

import com.minidb.protocol.Protocol;
import java.net.URI;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class MiniDbDriver implements Driver {

    static {
        try {
            DriverManager.registerDriver(new MiniDbDriver());
        } catch (SQLException e) {
            throw new RuntimeException("failed to register MiniDbDriver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        URI uri = URI.create(url.substring("jdbc:".length()));
        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : Protocol.DEFAULT_PORT;
        long timeout = parseTimeout(uri.getQuery());
        MiniDbClient client = new MiniDbClient(timeout);
        try {
            client.connect(host, port);
        } catch (SQLException e) {
            client.close();
            throw e;
        }
        return new MiniDbConnection(client, url);
    }

    private static long parseTimeout(String query) {
        if (query == null) {
            return 30;
        }
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && "timeout".equals(kv[0])) {
                try {
                    return Long.parseLong(kv[1]);
                } catch (NumberFormatException e) {
                    return 30;
                }
            }
        }
        return 30;
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith("jdbc:minidb://");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("com.minidb");
    }
}
