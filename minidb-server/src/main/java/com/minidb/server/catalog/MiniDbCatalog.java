package com.minidb.server.catalog;
import com.minidb.storage.common.TableSchema;

import com.minidb.server.stats.TableStats;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MiniDbCatalog {

    public static final String DEFAULT_SCHEMA = "public";

    private final Map<String, Map<String, TableSchema>> schemas = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ViewDefinition>> views = new ConcurrentHashMap<>();
    private final Map<String, TableStats> stats = new ConcurrentHashMap<>();
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    public MiniDbCatalog() {
        schemas.put(DEFAULT_SCHEMA, new ConcurrentHashMap<>());
        registerInformationSchema();
    }

    private void registerInformationSchema() {
        Map<String, TableSchema> systemTables = new ConcurrentHashMap<>();
        for (TableSchema table : InformationSchemaCatalog.tables()) {
            systemTables.put(key(table.name()), table);
        }
        schemas.put(key(InformationSchemaCatalog.SCHEMA_NAME), systemTables);
    }

    public void createSchema(String name) {
        String k = key(name);
        if (schemas.putIfAbsent(k, new ConcurrentHashMap<>()) != null) {
            throw new IllegalArgumentException("schema already exists: " + name);
        }
        notifyChange();
    }

    public void dropSchema(String name) {
        String k = key(name);
        if (k.equals(DEFAULT_SCHEMA)) {
            throw new IllegalArgumentException("cannot drop default schema: " + name);
        }
        if (k.equals(key(InformationSchemaCatalog.SCHEMA_NAME))) {
            throw new IllegalArgumentException("cannot drop system schema: " + name);
        }
        if (schemas.remove(k) == null) {
            throw new IllegalArgumentException("schema not found: " + name);
        }
        views.remove(k);
        stats.keySet().removeIf(key -> key.startsWith(k + "."));
        notifyChange();
    }

    public List<String> schemaNames() {
        return new ArrayList<>(schemas.keySet());
    }

    public void createTable(TableSchema schema) {
        String sk = key(schema.schemaName());
        Map<String, TableSchema> tables = schemas.get(sk);
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schema.schemaName());
        }
        String tk = key(schema.name());
        if (hasView(schema.schemaName(), schema.name())) {
            throw new IllegalArgumentException("view already exists: " + schema.name());
        }
        if (tables.putIfAbsent(tk, schema) != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        notifyChange();
    }

    public void createView(ViewDefinition view) {
        Map<String, ViewDefinition> v = views.computeIfAbsent(
                key(view.schemaName()), k -> new ConcurrentHashMap<>());
        if (hasTable(view.schemaName(), view.name())) {
            throw new IllegalArgumentException("table already exists: " + view.name());
        }
        if (v.putIfAbsent(key(view.name()), view) != null) {
            throw new IllegalArgumentException("view already exists: " + view.name());
        }
        notifyChange();
    }

    /** CREATE OR REPLACE VIEW:覆盖同 schema 下同名视图(不检查是否存在)。 */
    public void replaceView(ViewDefinition view) {
        Map<String, ViewDefinition> v = views.computeIfAbsent(
                key(view.schemaName()), k -> new ConcurrentHashMap<>());
        if (hasTable(view.schemaName(), view.name())) {
            throw new IllegalArgumentException("table already exists: " + view.name());
        }
        v.put(key(view.name()), view);
        notifyChange();
    }

    public void dropView(String schemaName, String viewName) {
        Map<String, ViewDefinition> v = views.get(key(schemaName));
        if (v == null || v.remove(key(viewName)) == null) {
            throw new IllegalArgumentException("view not found: " + viewName);
        }
        notifyChange();
    }

    public ViewDefinition getView(String schemaName, String viewName) {
        Map<String, ViewDefinition> v = views.get(key(schemaName));
        if (v == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        ViewDefinition view = v.get(key(viewName));
        if (view == null) {
            throw new IllegalArgumentException("view not found: " + viewName);
        }
        return view;
    }

    public boolean hasView(String schemaName, String viewName) {
        Map<String, ViewDefinition> v = views.get(key(schemaName));
        return v != null && v.containsKey(key(viewName));
    }

    public List<ViewDefinition> views(String schemaName) {
        Map<String, ViewDefinition> v = views.get(key(schemaName));
        return v == null ? List.of() : new ArrayList<>(v.values());
    }

    public void dropTable(String schemaName, String tableName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        if (tables.remove(key(tableName)) == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        stats.remove(statsKey(schemaName, tableName));
        notifyChange();
    }

    /** 替换一张表的不可变 TableSchema(列/约束变更),保留原表名。 */
    public void alterTable(String schemaName, String tableName, TableSchema newSchema) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        if (tables.get(key(tableName)) == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        tables.put(key(tableName), newSchema);
        notifyChange();
    }

    /** 改表名:新名不能与现有表/视图冲突,表结构原样保留。 */
    public void renameTable(String schemaName, String oldName, String newName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        String oldKey = key(oldName);
        TableSchema old = tables.get(oldKey);
        if (old == null) {
            throw new IllegalArgumentException("table not found: " + oldName);
        }
        String newKey = key(newName);
        if (hasView(schemaName, newName)) {
            throw new IllegalArgumentException("view already exists: " + newName);
        }
        if (tables.containsKey(newKey)) {
            throw new IllegalArgumentException("table already exists: " + newName);
        }
        TableSchema renamed = new TableSchema(old.schemaName(), newName, old.columns(),
                old.primaryKey(), old.uniqueKeys(), old.foreignKeys(), old.storageFormat(),
                old.tableType(), old.indexes());
        tables.remove(oldKey);
        tables.put(newKey, renamed);
        notifyChange();
    }

    public TableSchema getTable(String schemaName, String tableName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        TableSchema schema = tables.get(key(tableName));
        if (schema == null) {
            throw new IllegalArgumentException("table not found: " + tableName);
        }
        return schema;
    }

    public boolean hasTable(String schemaName, String tableName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        return tables != null && tables.containsKey(key(tableName));
    }

    public List<String> tableNames(String schemaName) {
        Map<String, TableSchema> tables = schemas.get(key(schemaName));
        if (tables == null) {
            throw new IllegalArgumentException("schema not found: " + schemaName);
        }
        List<String> names = new ArrayList<>();
        for (TableSchema schema : tables.values()) {
            names.add(schema.name());
        }
        return names;
    }

    public TableStats getStats(String schemaName, String tableName) {
        return stats.get(statsKey(schemaName, tableName));
    }

    public void setStats(String schemaName, String tableName, TableStats ts) {
        stats.put(statsKey(schemaName, tableName), ts);
        notifyChange();
    }

    public void markStatsStale(String schemaName, String tableName) {
        String k = statsKey(schemaName, tableName);
        TableStats ts = stats.get(k);
        if (ts != null) {
            stats.put(k, new TableStats(ts.columnHistograms(), ts.rowCount(), true));
            // 持久化 stale 标记:否则 DML 后重启会从 catalog.json 以 false 加载,
            // 把过期统计误判为新鲜。代价是每次 DML 写一次 catalog.json,玩具库可接受。
            notifyChange();
        }
    }

    private static String statsKey(String schemaName, String tableName) {
        return key(schemaName) + "." + key(tableName);
    }

    public void addListener(Runnable listener) {
        listeners.add(listener);
    }

    public CatalogSnapshot snapshot() {
        List<TableSchema> tables = new ArrayList<>();
        for (Map.Entry<String, Map<String, TableSchema>> e : schemas.entrySet()) {
            if (InformationSchemaCatalog.SCHEMA_NAME.equals(e.getKey())) {
                continue;
            }
            tables.addAll(e.getValue().values());
        }
        List<ViewDefinition> viewList = new ArrayList<>();
        for (Map.Entry<String, Map<String, ViewDefinition>> e : views.entrySet()) {
            if (InformationSchemaCatalog.SCHEMA_NAME.equals(e.getKey())) {
                continue;
            }
            viewList.addAll(e.getValue().values());
        }
        List<String> names = new ArrayList<>();
        for (String name : schemas.keySet()) {
            if (!InformationSchemaCatalog.SCHEMA_NAME.equals(name)) {
                names.add(name);
            }
        }
        return new CatalogSnapshot(names, tables, viewList, Map.copyOf(stats));
    }

    /** 批量恢复(启动时用),不触发 notifyChange —— 避免加载时把刚读到的文件写回。 */
    public void restore(CatalogSnapshot snapshot) {
        for (String schemaName : snapshot.schemas()) {
            schemas.putIfAbsent(key(schemaName), new ConcurrentHashMap<>());
        }
        for (TableSchema table : snapshot.tables()) {
            String sk = key(table.schemaName());
            Map<String, TableSchema> t = schemas.computeIfAbsent(sk, k -> new ConcurrentHashMap<>());
            t.putIfAbsent(key(table.name()), table);
        }
        for (ViewDefinition view : snapshot.views()) {
            String sk = key(view.schemaName());
            Map<String, ViewDefinition> v = views.computeIfAbsent(sk, k -> new ConcurrentHashMap<>());
            v.putIfAbsent(key(view.name()), view);
        }
        for (var e : snapshot.stats().entrySet()) {
            stats.put(e.getKey(), e.getValue());
        }
    }

    private void notifyChange() {
        for (Runnable listener : listeners) {
            listener.run();
        }
    }

    private static String key(String name) {
        return name.toLowerCase(Locale.ROOT);
    }
}
