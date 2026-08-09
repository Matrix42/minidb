package com.minidb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class MiniDbDriver implements Driver {
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        throw new UnsupportedOperationException("implemented in Task 11");
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
