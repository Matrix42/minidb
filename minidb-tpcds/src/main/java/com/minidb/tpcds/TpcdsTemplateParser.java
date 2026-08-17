package com.minidb.tpcds;

import java.io.IOException;
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

    public Map<String, String> parseAll(Path templateDir, double scale) throws IOException {
        Map<String, String> result = new TreeMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(templateDir, "query*.tpl")) {
            for (Path p : ds) {
                String fileName = p.getFileName().toString();
                String queryName = fileName.substring(0, fileName.length() - 4);
                int queryNumber = Integer.parseInt(queryName.substring("query".length()));
                result.put(queryName, parseTemplate(Files.readString(p), scale, queryNumber));
            }
        }
        return result;
    }

    public String parseTemplate(String tpl, double scale, int queryNumber) {
        random.setSeed(42);
        Map<String, Value> vars = new HashMap<>();
        // 方言默认(适配 Calcite 的 LIMIT):_LIMITA/_LIMITB 空,_LIMITC 展开为 limit n。
        vars.put("__LIMITA", new StrValue(""));
        vars.put("__LIMITB", new StrValue(""));
        vars.put("__LIMITC", new StrValue("limit %d"));
        StringBuilder sql = new StringBuilder();
        for (String line : tpl.split("\n")) {
            String trimmed = line.strip();
            if (trimmed.startsWith("define ")) {
                parseDefine(trimmed, vars, scale);
            } else {
                sql.append(line).append('\n');
            }
        }
        // 模板里的 ';' 是语句分隔符,Calcite 的 parseStmt 不接受分号,去掉。
        return replaceSubstitutions(sql.toString(), vars, queryNumber).replace(";", "");
    }

    private void parseDefine(String line, Map<String, Value> vars, double scale) {
        String body = line.substring("define ".length()).trim();
        int eq = body.indexOf('=');
        String name = body.substring(0, eq).trim();
        String expr = body.substring(eq + 1).trim();
        if (expr.endsWith(";")) {
            expr = expr.substring(0, expr.length() - 1);
        }
        vars.put(name, evalExpr(expr.trim(), vars, scale));
    }

    private Value evalExpr(String expr, Map<String, Value> vars, double scale) {
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
            return evalFunction(fn.group(1), fn.group(2), vars, scale);
        }
        // 算术/字符串拼接(一层):找最外层 + - * /。
        for (char op : new char[]{'+', '-', '*', '/'}) {
            int idx = topLevelOperator(expr, op);
            if (idx > 0) {
                Value left = evalExpr(expr.substring(0, idx), vars, scale);
                Value right = evalExpr(expr.substring(idx + 1), vars, scale);
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

    private Value evalFunction(String name, String args, Map<String, Value> vars, double scale) {
        return switch (name) {
            case "random" -> evalRandom(args, vars);
            case "rowcount" -> evalRowCount(args);
            case "distmember", "dist" -> evalDist(args, vars);
            case "text" -> evalText(args, vars);
            case "ulist", "list", "range" -> evalList(args, vars);
            case "date" -> new StrValue("2000-01-01");
            case "scalestep" -> new IntValue(1);
            default -> new StrValue("1");
        };
    }

    private Value evalRandom(String args, Map<String, Value> vars) {
        String[] parts = splitArgs(args);
        int a = asInt(evalExpr(parts[0], vars, 1.0));
        int b = asInt(evalExpr(parts[1], vars, 1.0));
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
        int columnIdx = parts.length >= 2 ? asInt(evalExpr(parts[1], vars, 1.0)) : 1;
        return new StrValue(distCandidate(distName, columnIdx));
    }

    private Value evalText(String args, Map<String, Value> vars) {
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
            list.add(evalExpr(parts[0], vars, 1.0));
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
        StringBuffer sb = new StringBuffer();
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
        if (cur.length() > 0) {
            args.add(cur.toString().trim());
        }
        return args.toArray(String[]::new);
    }
}
