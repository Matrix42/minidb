package com.minidb.jdbc;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MiniDbResultSet implements ResultSet {

    private final MiniDbStatement statement;
    private final MiniDbClient client;
    private final java.sql.ResultSetMetaData metaData;
    private VectorSchemaRoot root;
    private int cursor = -1;
    private boolean wasNull;
    private boolean closed;
    private long cursorId;
    private int fetchSize;
    private boolean lastBatch;
    private int rowNumber;
    private boolean exhausted;
    // DatabaseMetaData 内部创建的 Statement:用户拿不到引用,关闭 ResultSet 时一并关闭
    // (否则每次 getTables/getColumns/getSchemas/getTableTypes 泄漏一个 Statement)。
    private final boolean closeStatementOnClose;

    public MiniDbResultSet(MiniDbStatement statement, VectorSchemaRoot root) {
        this(statement, root, false);
    }

    public MiniDbResultSet(
            MiniDbStatement statement, VectorSchemaRoot root, boolean closeStatementOnClose) {
        this.statement = statement;
        this.client = null;
        this.root = root;
        this.lastBatch = true;
        this.closeStatementOnClose = closeStatementOnClose;
        this.metaData = new MiniDbResultSetMetaData(root);
    }

    public MiniDbResultSet(
            MiniDbStatement statement,
            MiniDbClient client,
            MiniDbClient.ClientResult.Cursor cursor) {
        this.statement = statement;
        this.client = client;
        this.root = cursor.firstPage();
        this.cursorId = cursor.cursorId();
        this.fetchSize = cursor.fetchSize();
        this.lastBatch = cursor.lastBatch();
        this.closeStatementOnClose = false;
        this.metaData = new MiniDbResultSetMetaData(this.root);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        while (true) {
            cursor++;
            if (cursor < root.getRowCount()) {
                rowNumber++;
                return true;
            }
            if (lastBatch) {
                exhausted = true;
                return false;
            }
            MiniDbClient.ClientResult.Rows page = client.fetch(cursorId, fetchSize);
            root.close();
            root = page.data();
            lastBatch = page.lastBatch();
            cursor = -1;
        }
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("result set is closed");
        }
    }

    private ValueVector vector(int columnIndex) throws SQLException {
        checkClosed();
        if (columnIndex < 1 || columnIndex > root.getFieldVectors().size()) {
            throw new SQLException("column index out of range: " + columnIndex);
        }
        return root.getVector(columnIndex - 1);
    }

    private boolean isNull(ValueVector v) {
        wasNull = v.isNull(cursor);
        return wasNull;
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) {
            return 0;
        }
        if (v instanceof IntVector iv) {
            return iv.get(cursor);
        }
        if (v instanceof BigIntVector bv) {
            return (int) bv.get(cursor);
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(cursor);
        }
        throw new SQLException("not an integer column");
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) {
            return 0L;
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(cursor);
        }
        if (v instanceof IntVector iv) {
            return iv.get(cursor);
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(cursor);
        }
        throw new SQLException("not a bigint column");
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) {
            return 0d;
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(cursor);
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(cursor);
        }
        if (v instanceof IntVector iv) {
            return iv.get(cursor);
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(cursor);
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(cursor);
        }
        throw new SQLException("not a double column");
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) {
            return false;
        }
        if (v instanceof BitVector bv) {
            return bv.get(cursor) == 1;
        }
        // JDBC 规范:数值列 0=false,非 0=true。
        if (v instanceof IntVector iv) {
            return iv.get(cursor) != 0;
        }
        if (v instanceof BigIntVector bv) {
            return bv.get(cursor) != 0L;
        }
        if (v instanceof SmallIntVector sv) {
            return sv.get(cursor) != 0;
        }
        if (v instanceof Float8Vector fv) {
            return fv.get(cursor) != 0d;
        }
        if (v instanceof Float4Vector fv) {
            return fv.get(cursor) != 0f;
        }
        throw new SQLException("not a boolean column");
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) {
            return null;
        }
        if (v instanceof VarCharVector vv) {
            return new String(vv.get(cursor));
        }
        // JDBC 规范 getString 是通用 getter:非 VARCHAR 列按 getObject 的语义转字符串。
        return String.valueOf(getObject(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) {
            return null;
        }
        if (v instanceof DateDayVector dv) {
            int days = dv.get(cursor);
            return new Date(TimeUnit.DAYS.toMillis(days));
        }
        throw new SQLException("not a date column");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) {
            return null;
        }
        if (v instanceof TimeStampMilliVector tv) {
            return new Timestamp(tv.get(cursor));
        }
        throw new SQLException("not a timestamp column");
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        // 注意:vector() 返回的是 root 持有的共享向量,不能 try-with-resources 关闭——
        // 关闭共享向量会释放整个列的 buffer,后续行访问直接越界(IndexOutOfBoundsException)。
        // 与 getInt/getString 等一样,不在这里释放。
        ValueVector v = vector(columnIndex);
        if (v.isNull(cursor)) {
            wasNull = true;
            return null;
        }
        return switch (v.getMinorType()) {
            case INT -> getInt(columnIndex);
            case BIGINT -> getLong(columnIndex);
            case FLOAT8 -> getDouble(columnIndex);
            case VARCHAR -> getString(columnIndex);
            case BIT -> getBoolean(columnIndex);
            case DATEDAY -> getDate(columnIndex);
            case TIMESTAMPMILLI -> getTimestamp(columnIndex);
            case SMALLINT -> getShort(columnIndex);
            case FLOAT4 -> getFloat(columnIndex);
            case DECIMAL -> getBigDecimal(columnIndex);
            case TIMEMILLI -> getTime(columnIndex);
            case VARBINARY -> getBytes(columnIndex);
            default -> throw new SQLException("unsupported type: " + v.getMinorType());
        };
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        for (int i = 0; i < root.getFieldVectors().size(); i++) {
            if (root.getFieldVectors().get(i).getName().equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("no column named " + columnLabel);
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            root.close();
            if (client != null && !lastBatch) {
                client.closeCursor(cursorId);
            }
            if (closeStatementOnClose && statement != null) {
                try {
                    statement.close();
                } catch (SQLException ignored) {
                    // 尽力关闭
                }
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public int getRow() {
        return rowNumber;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {}

    @Override
    public int getType() {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getHoldability() {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        try {
            Object o = getObject(columnIndex);
            return type.cast(o);
        } catch (ClassCastException e) {
            // JDBC 规范:类型不匹配应抛 SQLException,而非运行时 ClassCastException,
            // 否则调用方 catch(SQLException) 会漏掉。
            throw new SQLException(
                    "cannot convert column " + columnIndex + " to " + type.getName(), e);
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        try {
            Object o = getObject(columnLabel);
            return type.cast(o);
        } catch (ClassCastException e) {
            throw new SQLException(
                    "cannot convert column " + columnLabel + " to " + type.getName(), e);
        }
    }

    // --- Unsupported: scrolling, updates, streams, advanced types ---

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) return null;
        if (v instanceof DecimalVector dv) return dv.getObject(cursor);
        if (v instanceof Float8Vector fv) return BigDecimal.valueOf(fv.get(cursor));
        if (v instanceof Float4Vector fv) return BigDecimal.valueOf(fv.get(cursor));
        if (v instanceof BigIntVector bv) return BigDecimal.valueOf(bv.get(cursor));
        if (v instanceof IntVector iv) return BigDecimal.valueOf(iv.get(cursor));
        if (v instanceof SmallIntVector sv) return BigDecimal.valueOf(sv.get(cursor));
        throw new SQLException("not a decimal column");
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) return null;
        if (v instanceof VarBinaryVector bv) return bv.get(cursor);
        throw new SQLException("not a binary column");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) return 0;
        long value;
        if (v instanceof SmallIntVector sv) {
            value = sv.get(cursor);
        } else if (v instanceof IntVector iv) {
            value = iv.get(cursor);
        } else if (v instanceof BigIntVector bv) {
            value = bv.get(cursor);
        } else {
            throw new SQLException("not an integer column");
        }
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new SQLException("value out of range for byte: " + value);
        }
        return (byte) value;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) return 0;
        if (v instanceof SmallIntVector sv) return sv.get(cursor);
        if (v instanceof IntVector iv) return (short) iv.get(cursor);
        throw new SQLException("not a smallint column");
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) return 0f;
        if (v instanceof Float4Vector fv) return fv.get(cursor);
        if (v instanceof Float8Vector fv) return (float) fv.get(cursor);
        throw new SQLException("not a float column");
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        ValueVector v = vector(columnIndex);
        if (isNull(v)) return null;
        if (v instanceof TimeMilliVector tv) {
            // TimeMilliVector 存的是当日毫秒数(12:00 → 43200000),不能直接 new Time(millisOfDay)
            // 按 UTC epoch millis 解释——那会让非 UTC 时区的 toString() 偏移(UTC+8 显示 20:00:00)。
            // 用 LocalTime.ofNanoOfDay 构造,getTime() 返回的是「今日此刻的 epoch 毫秒」,
            // toString()/toLocalTime() 均回到正确的当日时刻。
            long millisOfDay = tv.get(cursor);
            return Time.valueOf(java.time.LocalTime.ofNanoOfDay(millisOfDay * 1_000_000L));
        }
        throw new SQLException("not a time column");
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    // --- Scrolling (forward-only) ---

    @Override
    public boolean isBeforeFirst() {
        return !exhausted && rowNumber == 0;
    }

    @Override
    public boolean isAfterLast() {
        return exhausted;
    }

    @Override
    public boolean isFirst() {
        return !exhausted && rowNumber == 1;
    }

    @Override
    public boolean isLast() {
        // Page-local: the absolute last row isn't knowable for a forward-only
        // cursor without lookahead (fetching the next page to see if it's empty).
        // JDBC is lenient about isLast() for TYPE_FORWARD_ONLY.
        return cursor == root.getRowCount() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) {}

    @Override
    public int getFetchDirection() {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) {}

    @Override
    public int getFetchSize() {
        return 0;
    }

    // --- Updates (read-only) ---

    @Override
    public boolean rowUpdated() {
        return false;
    }

    @Override
    public boolean rowInserted() {
        return false;
    }

    @Override
    public boolean rowDeleted() {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream, long length)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
