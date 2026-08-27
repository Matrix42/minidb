package com.minidb.jdbc;

import com.minidb.protocol.Message;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class MiniDbConnection implements Connection {

    private final MiniDbClient client;
    private final String url;
    // 本连接创建过、尚未关闭的 Statement/PreparedStatement。JDBC 规范要求关闭 Connection
    // 连带关闭其创建的所有 Statement(及其 ResultSet)。并发关闭遍历需线程安全。
    private final Set<Statement> openStatements = ConcurrentHashMap.newKeySet();
    private boolean closed;
    // 当前 schema(JDBC 语义);默认 public。setSchema/getSchema 与服务端 USE SCHEMA 对接。
    private String currentSchema = "public";
    private int transactionIsolation = Connection.TRANSACTION_SERIALIZABLE;
    private boolean autoCommit = true;

    public MiniDbConnection(MiniDbClient client, String url) {
        this.client = client;
        this.url = url;
    }

    MiniDbClient client() {
        return client;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        MiniDbStatement stmt = new MiniDbStatement(this, client);
        openStatements.add(stmt);
        return stmt;
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        MiniDbPreparedStatement stmt = new MiniDbPreparedStatement(this, client, sql);
        openStatements.add(stmt);
        return stmt;
    }

    /** Statement 主动 close() 时从此连接注销(包内可见,供 MiniDbStatement 回调)。 */
    void statementClosed(Statement statement) {
        openStatements.remove(statement);
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            // 先关闭所有 statement(连带其 ResultSet 与 Arrow 资源),再关闭 client。
            for (Statement stmt : openStatements) {
                try {
                    stmt.close();
                } catch (SQLException ignored) {
                    // 尽力关闭,不因单个 statement 失败而中断其余清理
                }
            }
            openStatements.clear();
            client.close();
        }
    }

    @Override
    public boolean isClosed() {
        // closed reflects either an explicit close() OR the underlying channel
        // going inactive (server closed, network dropped). Without this, a pool
        // would keep handing out dead connections until a query actually failed.
        return closed || !client.isConnected();
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.autoCommit == autoCommit) {
            return;
        }
        long id = client.nextRequestId();
        client.sendAndWait(id, new Message.SetAutoCommitRequest(id, autoCommit));
        this.autoCommit = autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            return;
        }
        long id = client.nextRequestId();
        client.sendAndWait(id, new Message.CommitRequest(id));
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            return;
        }
        long id = client.nextRequestId();
        client.sendAndWait(id, new Message.RollbackRequest(id));
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new MiniDbDatabaseMetaData(this, url);
    }

    @Override
    public boolean isValid(int timeout) {
        return !isClosed();
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("connection is closed");
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public void setCatalog(String catalog) {
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) {
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() {
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) {
    }

    @Override
    public int getHoldability() {
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
    }

    @Override
    public String getClientInfo(String name) {
        return null;
    }

    @Override
    public Properties getClientInfo() {
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        if (schema == null) {
            throw new SQLException("schema must not be null");
        }
        // 服务端由 USE SCHEMA 负责校验存在性并切换此连接的 currentSchema。
        // MiniDB 的 schema 名是简单标识符(服务端统一 lowercase 解析),直接透传裸名,
        // 不加引号(服务端 USE SCHEMA 处理器不剥离引号)。执行成功才记录本地 schema。
        try (Statement stmt = createStatement()) {
            stmt.execute("USE SCHEMA " + schema);
        }
        this.currentSchema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return currentSchema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) {
    }

    @Override
    public int getNetworkTimeout() {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
