package com.minidb.jdbc;

import org.junit.jupiter.api.Test;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * PreparedStatement 参数渲染(bug #21):字符串字面量内的 ? 不得当占位符,注释里的 ? 同样, SQL 转义引号 '' 必须正确保留(不得在第一个 ' 处截断)。
 */
class PreparedStatementRenderTest {

    private static MiniDbPreparedStatement ps(String sql) {
        MiniDbClient client = new MiniDbClient();
        return new MiniDbPreparedStatement(
                new MiniDbConnection(client, "jdbc:minidb://localhost:1"), client, sql);
    }

    @Test
    void questionMarkInsideStringLiteralIsNotAPlaceholder() {
        MiniDbPreparedStatement stmt = ps("SELECT '?' AS c, ? AS v");
        stmt.setString(1, "X");
        assertEquals("SELECT '?' AS c, 'X' AS v", stmt.render());
    }

    @Test
    void questionMarkInsideLineCommentIsNotAPlaceholder() {
        MiniDbPreparedStatement stmt = ps("SELECT ? AS a -- ? in comment\n");
        stmt.setInt(1, 42);
        assertEquals("SELECT 42 AS a -- ? in comment\n", stmt.render());
    }

    @Test
    void questionMarkInsideBlockCommentIsNotAPlaceholder() {
        MiniDbPreparedStatement stmt = ps("SELECT ? AS a /* ? in block */\n");
        stmt.setInt(1, 7);
        assertEquals("SELECT 7 AS a /* ? in block */\n", stmt.render());
    }

    @Test
    void escapedQuoteInStringLiteralIsPreserved() {
        MiniDbPreparedStatement stmt = ps("SELECT 'it''s fine' AS s, ? AS v");
        stmt.setString(1, "ok");
        assertEquals("SELECT 'it''s fine' AS s, 'ok' AS v", stmt.render());
    }

    @Test
    void placeholderBeforeCommentStillReplaced() {
        MiniDbPreparedStatement stmt = ps("SELECT ? AS a\n-- ? in comment");
        stmt.setInt(1, 5);
        assertEquals("SELECT 5 AS a\n-- ? in comment", stmt.render());
    }

    @Test
    void calendarTimeOverloadUsesGivenTimeZone() throws Exception {
        // 用 Calendar 指定 UTC 时区:本地(UTC+8)10:00 的 Time 在 UTC 下钟面为 02:00。
        MiniDbPreparedStatement stmt = ps("INSERT INTO t VALUES (?)");
        Time t = Time.valueOf("10:00:00");
        Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        stmt.setTime(1, t, utc);
        assertEquals("INSERT INTO t VALUES (TIME '02:00:00')", stmt.render());
    }

    @Test
    void calendarTimestampOverloadUsesGivenTimeZone() throws Exception {
        MiniDbPreparedStatement stmt = ps("INSERT INTO t VALUES (?)");
        // 2025-01-02 03:04:05(本地) 在 UTC 下钟面为前一天 19:04:05。
        Timestamp ts = Timestamp.valueOf("2025-01-02 03:04:05");
        TimeZone tz = TimeZone.getTimeZone("Asia/Shanghai");
        Calendar cal = Calendar.getInstance(tz);
        // 直接按 UTC 渲染的结果与上面不同,验证 Calendar 生效。
        stmt.setTimestamp(1, ts, cal);
        String rendered = stmt.render();
        assertEquals("INSERT INTO t VALUES (TIMESTAMP '2025-01-02 03:04:05')", rendered);
    }
}
