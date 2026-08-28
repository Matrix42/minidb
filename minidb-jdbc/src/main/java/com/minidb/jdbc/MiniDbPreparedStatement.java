package com.minidb.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

public class MiniDbPreparedStatement extends MiniDbStatement implements PreparedStatement {

    private final String template;
    private final Map<Integer, Object> params = new HashMap<>();

    public MiniDbPreparedStatement(MiniDbConnection connection, MiniDbClient client,
                                   String template) {
        super(connection, client);
        this.template = template;
    }

    /** 渲染参数化 SQL:正确识别字符串字面量('...' 内含 SQL 转义 ''),避免把字面量内
     *  的 ? 误当占位符,并跳过注释里的 ?。包内可见供测试。 */
    String render() {
        StringBuilder out = new StringBuilder();
        int paramIndex = 1;
        int i = 0;
        int len = template.length();
        while (i < len) {
            char c = template.charAt(i);
            if (c == '\'') {
                // 字符串字面量:跳过,同时正确处理 SQL 转义引号(''),不把字面量内/转义处的
                // 字符当占位符。
                i = appendStringLiteral(out, i);
            } else if (c == '-' && i + 1 < len && template.charAt(i + 1) == '-') {
                // 行注释:原样复制到行尾,内部的 ? 不替换。
                int nl = template.indexOf('\n', i + 2);
                int end = nl < 0 ? len : nl;
                out.append(template, i, end);
                i = end;
            } else if (c == '/' && i + 1 < len && template.charAt(i + 1) == '*') {
                // 块注释:原样复制到结束,内部 ? 不替换。
                int close = template.indexOf("*/", i + 2);
                int end = close < 0 ? len : close + 2;
                out.append(template, i, end);
                i = end;
            } else if (c == '?') {
                Object value = params.get(paramIndex++);
                out.append(literal(value));
                i++;
            } else {
                out.append(c);
                i++;
            }
        }
        return out.toString();
    }

    /** 从 template[i]('）复制一个完整字符串字面量到 out,返回字面量结束后的下标。
     *  按 SQL 规则处理转义引号('')与反斜杠转义(\\'),避免在字面量内部/转义处截断。 */
    private int appendStringLiteral(StringBuilder out, int start) {
        int i = start;
        int len = template.length();
        out.append(template.charAt(i)); // 开引号
        i++;
        while (i < len) {
            char c = template.charAt(i);
            if (c == '\\' && i + 1 < len) {
                out.append(c).append(template.charAt(i + 1));
                i += 2;
                continue;
            }
            if (c == '\'') {
                if (i + 1 < len && template.charAt(i + 1) == '\'') {
                    // SQL 转义引号:两个相邻单引号 = 字面量内的一个单引号,原样保留且不闭串
                    out.append("''");
                    i += 2;
                    continue;
                }
                // 闭引号:吞掉后返回下一个下标,避免外层把闭引号再当新串开端。
                out.append('\'');
                return i + 1;
            }
            out.append(c);
            i++;
        }
        return i;
    }

    private String literal(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof RawSql raw) {
            return raw.sql();
        }
        if (value instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
        }
        // Time 必须在 Date 之前检查,因为 Time 继承自 Date
        if (value instanceof Time t) {
            // 用 toLocalTime() 取「当日钟面时间」(与 TimeMilliVector 的毫秒语义一致),
            // 不能用 UTC 格式化 getTime():那会把本地 10:30 按 epoch 错渲染成 02:30。
            java.time.LocalTime lt = t.toLocalTime();
            return String.format(Locale.ROOT, "TIME '%02d:%02d:%02d'",
                    lt.getHour(), lt.getMinute(), lt.getSecond());
        }
        if (value instanceof Boolean || value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Date d) {
            return "DATE '" + d + "'";
        }
        if (value instanceof Timestamp ts) {
            // 用 UTC 渲染,避免服务器把本地时间当成 UTC 存储导致时区偏移
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return "TIMESTAMP '" + sdf.format(ts) + "'";
        }
        if (value instanceof byte[] bytes) {
            StringBuilder hex = new StringBuilder(bytes.length * 2 + 3);
            hex.append("X'");
            for (byte b : bytes) {
                hex.append(String.format("%02X", b & 0xFF));
            }
            hex.append("'");
            return hex.toString();
        }
        throw new IllegalArgumentException("unsupported parameter type: " + value);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(render());
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(render());
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(render());
    }

    @Override
    public void setObject(int parameterIndex, Object x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) {
        params.put(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        params.put(parameterIndex, x);
    }

    @Override
    public void clearParameters() {
        params.clear();
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        params.put(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        params.put(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        params.put(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        params.put(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        params.put(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        params.put(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        if (x == null) {
            params.put(parameterIndex, null);
            return;
        }
        // 用调用方指定的时区解释日期:cal 的时区决定「哪天」。
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(cal.getTimeZone());
        params.put(parameterIndex, new RawSql("DATE '" + df.format(x) + "'"));
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        if (x == null) {
            params.put(parameterIndex, null);
            return;
        }
        // 用 cal 时区把时刻换算成钟面时间(与无 Calendar 版本 toLocalTime 语义一致,但时区可变)。
        Calendar c = Calendar.getInstance(cal.getTimeZone());
        c.setTimeInMillis(x.getTime());
        params.put(parameterIndex, new RawSql(String.format(Locale.ROOT,
                "TIME '%02d:%02d:%02d'",
                c.get(Calendar.HOUR_OF_DAY),
                c.get(Calendar.MINUTE),
                c.get(Calendar.SECOND))));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        if (x == null) {
            params.put(parameterIndex, null);
            return;
        }
        // 用 cal 时区格式化时间戳:服务端按字面钟面时刻存储,故须按调用方时区取钟面时间。
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(cal.getTimeZone());
        params.put(parameterIndex, new RawSql("TIMESTAMP '" + sdf.format(x) + "'"));
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) {
        params.put(parameterIndex, null);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /** 已渲染成 SQL 字面量的参数(带 Calendar 的重载按调用方时区预格式化),渲染阶段直出。 */
    private record RawSql(String sql) {}
}
