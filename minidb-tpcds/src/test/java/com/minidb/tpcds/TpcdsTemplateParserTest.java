package com.minidb.tpcds;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TpcdsTemplateParserTest {

    @Test
    void parsesDefineAndSubstitution() {
        TpcdsTemplateParser parser = new TpcdsTemplateParser();
        String tpl = "define YEAR = random(1998, 2002, uniform);\n\n"
                + "select * from t where d_year = [YEAR];\n";
        String sql = parser.parseTemplate(tpl, 1.0, 1);
        assertFalse(sql.contains("define"), "define 应被移除");
        assertFalse(sql.contains("["), "替换应完成: " + sql);
        assertTrue(sql.contains("d_year = "), "应保留 SQL 主体");
    }

    @Test
    void reproducible() {
        TpcdsTemplateParser parser = new TpcdsTemplateParser();
        String tpl = "define X = random(1, 100, uniform);\nselect [X];\n";
        assertEquals(parser.parseTemplate(tpl, 1.0, 1), parser.parseTemplate(tpl, 1.0, 1));
    }

    @Test
    void textPicksCandidate() {
        TpcdsTemplateParser parser = new TpcdsTemplateParser();
        String tpl = "define AGG = text({\"a\",1},{\"b\",1});\nselect [AGG] from t;\n";
        String sql = parser.parseTemplate(tpl, 1.0, 1);
        assertTrue(sql.contains("select a") || sql.contains("select b"), sql);
    }

    @Test
    void ulistAndIndexedRef() {
        TpcdsTemplateParser parser = new TpcdsTemplateParser();
        String tpl = "define N = ulist(random(1, 1000, uniform), 3);\n"
                + "select [N.1], [N.2], [N.3];\n";
        String sql = parser.parseTemplate(tpl, 1.0, 1);
        assertFalse(sql.contains("[N"), "应替换 [N.N]: " + sql);
        assertTrue(sql.matches("(?s)select \\d+, \\d+, \\d+;.*"), sql);
    }

    @Test
    void limitExpansion() {
        TpcdsTemplateParser parser = new TpcdsTemplateParser();
        String tpl = "define _LIMIT = 100;\nselect * from t [_LIMITC];\n";
        String sql = parser.parseTemplate(tpl, 1.0, 1);
        assertTrue(sql.contains("limit 100"), "应展开 limit: " + sql);
    }

    @Test
    void parsesAllRealTemplates() throws Exception {
        Path dir = Path.of("F:/DSGen-software-code-4.0.0/query_templates");
        if (!Files.isDirectory(dir)) {
            return; // 无 DSGen 环境,跳过
        }
        TpcdsTemplateParser parser = new TpcdsTemplateParser();
        Map<String, String> sqls = parser.parseAll(dir, 1.0);
        assertEquals(99, sqls.size());
        for (Map.Entry<String, String> e : sqls.entrySet()) {
            String sql = e.getValue();
            assertFalse(sql.contains("define "), e.getKey() + " 残留 define");
            assertFalse(sql.matches("(?s).*\\[[A-Za-z_].*"), e.getKey() + " 残留占位符: " + sql);
        }
    }
}
