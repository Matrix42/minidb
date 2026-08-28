package com.minidb.tpcds;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TPC-DS 查询模板解析器:把 DSGen 的 {@code queryNN.tpl} 转成可执行的 SQL。
 * 求值策略是「简化实现」——固定 seed 保证可复现,函数返回类型正确且在数据范围内
 * 的候选值,不追求与官方 qgen 的精确分布一致(目的是测耗时,不校验 answer set)。
 */
public class TpcdsTemplateParser {

    private final Random random = new Random(42);

    /** rowcount 引用的分布/表名 → 大小(简化:固定值,仅影响 random 上界)。 */
    private static final Map<String, Integer> ROW_COUNTS = Map.of(
            "active_counties", 3000,
            "active_states", 50,
            "store", 1000,
            "store_sales", 1000000,
            "web_sales", 1000000,
            "catalog_sales", 1000000);

    private sealed interface Value permits IntValue, StrValue, ListValue {
    }

    private record IntValue(int v) implements Value {
    }

    private record StrValue(String s) implements Value {
    }

    private record ListValue(List<Value> list) implements Value {
    }

    private static final Pattern VAR_REF =
            Pattern.compile("\\[([A-Za-z_][A-Za-z_0-9]*)(?:\\.([0-9]+))?\\]");

    public Map<String, String> parseAll(Path templateDir) throws IOException {
        // 数字序(query1, query2, ..., query99),而非字典序(query1, query10, ...)。
        Map<String, String> result = new TreeMap<>((a, b) ->
                Integer.compare(queryNumber(a), queryNumber(b)));
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(templateDir, "query*.tpl")) {
            for (Path p : ds) {
                String fileName = p.getFileName().toString();
                String queryName = fileName.substring(0, fileName.length() - 4);
                result.put(queryName, parseTemplate(Files.readString(p), queryNumber(queryName)));
            }
        }
        return result;
    }

    private static int queryNumber(String name) {
        return Integer.parseInt(name.substring("query".length()));
    }

    /** 内置模板在 classpath 里的资源目录。 */
    private static final String BUNDLED_TEMPLATE_DIR = "/tpcds/query_templates/";

    /**
     * 从模块内置 resources 读 99 个查询模板(无需外部 DSGen 工具),语义同 {@link #parseAll}。
     */
    public Map<String, String> parseBundled() throws IOException {
        Map<String, String> result = new TreeMap<>((a, b) ->
                Integer.compare(queryNumber(a), queryNumber(b)));
        for (int i = 1; i <= 99; i++) {
            String name = "query" + i + ".tpl";
            try (InputStream in = TpcdsTemplateParser.class
                    .getResourceAsStream(BUNDLED_TEMPLATE_DIR + name)) {
                if (in == null) {
                    throw new IOException("内置模板缺失: " + BUNDLED_TEMPLATE_DIR + name);
                }
                result.put("query" + i,
                        parseTemplate(new String(in.readAllBytes(), StandardCharsets.UTF_8), i));
            }
        }
        return result;
    }

    public String parseTemplate(String tpl, int queryNumber) {
        random.setSeed(42);
        Map<String, Value> vars = new HashMap<>();
        // 方言默认(适配 Calcite 的 LIMIT):_LIMITA/_LIMITB 空,_LIMITC 展开为 limit n。
        vars.put("__LIMITA", new StrValue(""));
        vars.put("__LIMITB", new StrValue(""));
        vars.put("__LIMITC", new StrValue("limit %d"));
        StringBuilder sql = new StringBuilder();
        String[] lines = tpl.split("\n");
        for (int i = 0; i < lines.length; i++) {
            String trimmed = lines[i].strip();
            if (trimmed.startsWith("define ")) {
                // 跨行 define(如 text 候选跨多行):收集到分号结束再求值。
                StringBuilder def = new StringBuilder(trimmed);
                while (!trimmed.endsWith(";") && i + 1 < lines.length) {
                    i++;
                    def.append(' ').append(lines[i].strip());
                    trimmed = def.toString();
                }
                parseDefine(def.toString(), vars);
            } else {
                sql.append(lines[i]).append('\n');
            }
        }
        String text = replaceSubstitutions(sql.toString(), vars, queryNumber);
        // TPC-DS 的 `date + N days` 依赖 date arithmetic(MiniDB 内核暂不支持),基准测试只测
        // 耗时不校验结果,故删掉偏移量让查询可执行(日期范围略偏,不影响「能否执行」)。
        text = text.replaceAll("[+-]\\s*\\d+\\s+days", "");
        // 裸整数偏移(如 query72 的 d1.d_date + 5):内核同样不支持 date+integer/interval,
        // 一并删掉偏移量。(?!…)避免误伤 d_date_sk。
        text = text.replaceAll("(d_date)(?![a-z0-9_])\\s*[+-]\\s*\\d+", "$1");
        // TPC-DS 里部分保留关键字被用作标识符(as returns/sum(returns)/as year),Calcite
        // 解析报错,加双引号转成标识符(\b 保证不误伤 store_returns 里的 returns)。
        text = text.replaceAll("(?i)\\b(returns|year|at)\\b", "\"$1\"");
        // 模板里部分查询(query14/23/24/39)含两个独立语句(第二个是变体),截断到第一个分号
        // 只保留首个;单语句查询的分号也一并去掉(Calcite 不接受分号)。
        int semi = text.indexOf(';');
        return semi >= 0 ? text.substring(0, semi) : text;
    }

    private void parseDefine(String line, Map<String, Value> vars) {
        String body = line.substring("define ".length()).trim();
        int eq = body.indexOf('=');
        String name = body.substring(0, eq).trim();
        String expr = body.substring(eq + 1).trim();
        if (expr.endsWith(";")) {
            expr = expr.substring(0, expr.length() - 1);
        }
        vars.put(name, evalExpr(expr.trim(), vars));
    }

    private Value evalExpr(String expr, Map<String, Value> vars) {
        expr = expr.trim();
        if (expr.matches("-?\\d+")) {
            return new IntValue(Integer.parseInt(expr));
        }
        if (expr.startsWith("\"") && expr.endsWith("\"")) {
            return new StrValue(expr.substring(1, expr.length() - 1));
        }
        Matcher varRef = VAR_REF.matcher(expr);
        if (varRef.matches()) {
            return evalVarRef(expr, vars);
        }
        Matcher fn = Pattern.compile("^([a-zA-Z_]+)\\((.*)\\)$", Pattern.DOTALL).matcher(expr);
        if (fn.matches()) {
            return evalFunction(fn.group(1), fn.group(2), vars);
        }
        // 算术/字符串拼接(一层):找最外层 + - * /。
        for (char op : new char[]{'+', '-', '*', '/'}) {
            int idx = topLevelOperator(expr, op);
            if (idx > 0) {
                Value left = evalExpr(expr.substring(0, idx), vars);
                Value right = evalExpr(expr.substring(idx + 1), vars);
                if (op == '+' && (left instanceof StrValue || right instanceof StrValue)) {
                    return new StrValue(valueToString(left) + valueToString(right));
                }
                int l = asInt(left);
                int r = asInt(right);
                return switch (op) {
                    case '+' -> new IntValue(l + r);
                    case '-' -> new IntValue(l - r);
                    case '*' -> new IntValue(l * r);
                    default -> new IntValue(r == 0 ? 0 : l / r);
                };
            }
        }
        // 未识别:返回字符串原样(保守,避免 NPE)。
        return new StrValue(expr);
    }

    private Value evalVarRef(String expr, Map<String, Value> vars) {
        Matcher m = VAR_REF.matcher(expr);
        if (!m.matches()) {
            return new StrValue("");
        }
        Value v = vars.get(m.group(1));
        if (v instanceof ListValue lv && m.group(2) != null) {
            int idx = Integer.parseInt(m.group(2)) - 1;
            return idx >= 0 && idx < lv.list().size() ? lv.list().get(idx) : new IntValue(0);
        }
        return v != null ? v : new StrValue("");
    }

    private Value evalFunction(String name, String args, Map<String, Value> vars) {
        return switch (name) {
            case "random" -> evalRandom(args, vars);
            case "rowcount" -> evalRowCount(args);
            case "distmember", "dist" -> evalDist(args, vars);
            case "text" -> evalText(args);
            case "ulist", "list", "range" -> evalList(args, vars);
            case "date" -> new StrValue("2000-01-01");
            case "scalestep" -> new IntValue(1);
            default -> new StrValue("1");
        };
    }

    private Value evalRandom(String args, Map<String, Value> vars) {
        String[] parts = splitArgs(args);
        int a = asInt(evalExpr(parts[0], vars));
        int b = asInt(evalExpr(parts[1], vars));
        if (b < a) {
            return new IntValue(a);
        }
        return new IntValue(a + random.nextInt(b - a + 1));
    }

    private Value evalRowCount(String args) {
        String[] parts = splitArgs(args);
        String name = parts[0].trim().replace("\"", "").toLowerCase();
        return new IntValue(ROW_COUNTS.getOrDefault(name, 1000));
    }

    private Value evalDist(String args, Map<String, Value> vars) {
        String[] parts = splitArgs(args);
        String distName = parts[0].trim().replace("\"", "").toLowerCase();
        int columnIdx = parts.length >= 2 ? asInt(evalExpr(parts[1], vars)) : 1;
        return new StrValue(distCandidate(distName, columnIdx));
    }

    private Value evalText(String args) {
        // text({literal, weight}, ...):按权重随机选一个字符串。
        List<String[]> candidates = new ArrayList<>();
        for (String part : splitArgs(args)) {
            String p = part.trim();
            if (p.startsWith("{") && p.endsWith("}")) {
                String inner = p.substring(1, p.length() - 1);
                int comma = inner.lastIndexOf(',');
                if (comma > 0) {
                    String literal = inner.substring(0, comma).trim().replace("\"", "");
                    int weight = Integer.parseInt(inner.substring(comma + 1).trim());
                    candidates.add(new String[]{literal, String.valueOf(weight)});
                }
            }
        }
        if (candidates.isEmpty()) {
            return new StrValue("");
        }
        int total = candidates.stream().mapToInt(c -> Integer.parseInt(c[1])).sum();
        int pick = random.nextInt(total);
        int acc = 0;
        for (String[] c : candidates) {
            acc += Integer.parseInt(c[1]);
            if (pick < acc) {
                return new StrValue(c[0]);
            }
        }
        return new StrValue(candidates.get(0)[0]);
    }

    private Value evalList(String args, Map<String, Value> vars) {
        String[] parts = splitArgs(args);
        int n = Integer.parseInt(parts[1].trim());
        List<Value> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(evalExpr(parts[0], vars));
        }
        return new ListValue(list);
    }

    /** distmember/dist 返回的候选值:类型正确 + 大概率在数据范围内(不保证匹配)。 */
    private static String distCandidate(String distName, int columnIdx) {
        return switch (distName) {
            case "fips_county" -> columnIdx == 3 ? "CA" : columnIdx == 2 ? "Fayette County" : "5";
            case "cities" -> "Midway";
            case "marital_status" -> "M";
            case "education" -> "4 yr Degree";
            case "gender" -> "M";
            default -> "1";
        };
    }

    private String replaceSubstitutions(String text, Map<String, Value> vars, int queryNumber) {
        Matcher m = VAR_REF.matcher(text);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String replacement = resolve(m.group(1), m.group(2), vars, queryNumber);
            m.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    private String resolve(String name, String idx, Map<String, Value> vars, int queryNumber) {
        if (name.equals("_LIMITA") || name.equals("_LIMITB") || name.equals("_LIMITC")) {
            StrValue fmt = (StrValue) vars.getOrDefault("_" + name, new StrValue(""));
            int limit = vars.get("_LIMIT") instanceof IntValue iv ? iv.v() : 100;
            return fmt.s().contains("%d") ? String.format(fmt.s(), limit) : fmt.s();
        }
        switch (name) {
            case "QUERY" -> {
                return String.valueOf(queryNumber);
            }
            case "TEMPLATE" -> {
                return "query" + queryNumber + ".tpl";
            }
            case "SEED" -> {
                return "42";
            }
            case "STREAM" -> {
                return "0";
            }
            default -> {
            }
        }
        Value v = vars.get(name);
        if (v == null) {
            return "";
        }
        if (v instanceof ListValue lv) {
            if (idx != null) {
                int i = Integer.parseInt(idx) - 1;
                return i >= 0 && i < lv.list().size() ? valueToString(lv.list().get(i)) : "";
            }
            return lv.list().stream().map(this::valueToString).reduce((a, b) -> a + ", " + b).orElse("");
        }
        return valueToString(v);
    }

    private String valueToString(Value v) {
        if (v instanceof IntValue iv) {
            return String.valueOf(iv.v());
        }
        if (v instanceof StrValue sv) {
            return sv.s();
        }
        if (v instanceof ListValue lv) {
            return lv.list().stream().map(this::valueToString).reduce((a, b) -> a + ", " + b).orElse("");
        }
        return "";
    }

    private static int asInt(Value v) {
        return v instanceof IntValue iv ? iv.v() : 0;
    }

    /** 找最外层(括号/引号外)的运算符位置,找不到返回 -1。 */
    private static int topLevelOperator(String s, char op) {
        int depth = 0;
        boolean inQuote = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') {
                inQuote = !inQuote;
            }
            if (inQuote) {
                continue;
            }
            if (c == '(' || c == '[') {
                depth++;
            } else if (c == ')' || c == ']') {
                depth--;
            } else if (c == op && depth == 0) {
                return i;
            }
        }
        return -1;
    }

    /** 分割逗号分隔的参数,跳过括号/花括号/引号内的逗号。 */
    private static String[] splitArgs(String s) {
        List<String> args = new ArrayList<>();
        int depth = 0;
        boolean inQuote = false;
        StringBuilder cur = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c == '"') {
                inQuote = !inQuote;
            }
            if (!inQuote) {
                if (c == '(' || c == '{') {
                    depth++;
                } else if (c == ')' || c == '}') {
                    depth--;
                }
            }
            if (c == ',' && depth == 0 && !inQuote) {
                args.add(cur.toString().trim());
                cur.setLength(0);
            } else {
                cur.append(c);
            }
        }
        if (!cur.isEmpty()) {
            args.add(cur.toString().trim());
        }
        return args.toArray(String[]::new);
    }
}
