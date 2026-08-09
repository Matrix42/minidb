package com.minidb.server.exec;

import com.minidb.server.calcite.CalciteContext;
import com.minidb.server.catalog.ArrowTypes;
import com.minidb.server.catalog.ColumnMeta;
import com.minidb.server.catalog.ColumnType;
import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.catalog.TableSchema;
import com.minidb.server.plan.MiniDbModify;
import com.minidb.server.plan.MiniDbRel;
import com.minidb.server.plan.Planner;
import com.minidb.server.storage.StorageManager;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlDropTable;

public class QueryExecutor {

    private final MiniDbCatalog catalog;
    private final StorageManager storage;
    private final BufferAllocator allocator;
    private final Planner planner;
    private final CalciteContext calcite;

    public QueryExecutor(MiniDbCatalog catalog, StorageManager storage,
                         BufferAllocator allocator) {
        this.catalog = catalog;
        this.storage = storage;
        this.allocator = allocator;
        this.planner = new Planner(catalog);
        this.calcite = new CalciteContext(catalog);
    }

    public QueryResult execute(String sql) {
        SqlNode parsed = calcite.parse(sql);
        if (parsed instanceof SqlCreateTable create) {
            return handleCreate(create);
        }
        if (parsed instanceof SqlDropTable drop) {
            return handleDrop(drop);
        }
        RelNode plan = planner.plan(sql);
        ExecContext ctx = new ExecContext(storage, allocator);
        if (plan instanceof MiniDbModify modify) {
            try (BatchIterator it = modify.execute(ctx)) {
                while (it.hasNext()) {
                    it.next();
                }
                return new QueryResult.Update(modify.affected());
            }
        }
        try (BatchIterator it = ((MiniDbRel) plan).execute(ctx)) {
            return new QueryResult.Rows(materialize(it));
        }
    }

    private QueryResult handleCreate(SqlCreateTable create) {
        List<ColumnMeta> columns = new ArrayList<>();
        for (SqlNode columnNode : create.columnList) {
            SqlColumnDeclaration column = (SqlColumnDeclaration) columnNode;
            String typeName = column.dataType.getTypeName().getSimple();
            ColumnType type = ArrowTypes.fromSqlTypeName(typeName);
            columns.add(new ColumnMeta(column.name.getSimple(), type));
        }
        TableSchema schema = new TableSchema(create.name.getSimple(), columns);
        storage.createTable(schema);
        return new QueryResult.Update(0);
    }

    private QueryResult handleDrop(SqlDropTable drop) {
        String name = drop.name.getSimple();
        if (!catalog.hasTable(name)) {
            if (drop.ifExists) {
                return new QueryResult.Update(0);
            }
            throw new IllegalArgumentException("table not found: " + name);
        }
        storage.dropTable(name);
        return new QueryResult.Update(0);
    }

    private VectorSchemaRoot materialize(BatchIterator it) {
        VectorSchemaRoot merged = null;
        int dst = 0;
        while (it.hasNext()) {
            VectorSchemaRoot batch = it.next();
            if (merged == null) {
                merged = VectorSchemaRoot.create(batch.getSchema(), allocator);
                merged.allocateNew();
            }
            for (int i = 0; i < batch.getRowCount(); i++) {
                RowCopier.copyRow(batch, i, merged, dst++);
            }
        }
        if (merged == null) {
            throw new IllegalStateException("query produced no batches");
        }
        merged.setRowCount(dst);
        return merged;
    }
}
