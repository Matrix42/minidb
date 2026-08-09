package com.minidb.server.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MiniDbCatalog {

    private final Map<String, TableSchema> tables = new ConcurrentHashMap<>();
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    public void createTable(TableSchema schema) {
        TableSchema prev = tables.putIfAbsent(key(schema.name()), schema);
        if (prev != null) {
            throw new IllegalArgumentException("table already exists: " + schema.name());
        }
        notifyChange();
    }

    public void dropTable(String name) {
        TableSchema removed = tables.remove(key(name));
        if (removed == null) {
            throw new IllegalArgumentException("table not found: " + name);
        }
        notifyChange();
    }

    public TableSchema getTable(String name) {
        TableSchema schema = tables.get(key(name));
        if (schema == null) {
            throw new IllegalArgumentException("table not found: " + name);
        }
        return schema;
    }

    public boolean hasTable(String name) {
        return tables.containsKey(key(name));
    }

    public List<String> tableNames() {
        List<String> names = new ArrayList<>();
        for (TableSchema schema : tables.values()) {
            names.add(schema.name());
        }
        return names;
    }

    public void addListener(Runnable listener) {
        listeners.add(listener);
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
