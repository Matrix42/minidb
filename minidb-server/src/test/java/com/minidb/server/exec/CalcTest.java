package com.minidb.server.exec;
import com.minidb.storage.common.BatchIterator;

import com.minidb.server.catalog.MiniDbCatalog;
import com.minidb.server.plan.physical.MiniDbCalc;
import com.minidb.server.plan.physical.MiniDbConvention;
import com.minidb.server.plan.physical.MiniDbScan;
import com.minidb.server.plan.Planner;
import com.minidb.server.storage.StorageManager;
import com.minidb.server.stats.StatsManager;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;

import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * MiniDbCalc is defensive: regular SQL plans as Project/Filter, so LogicalCalc
 * never appears in practice. These tests drive the operator directly with a
 * hand-built RexProgram to prove projection + condition + renaming semantics.
 */
class CalcTest {

    @TempDir
    Path dataDir;

    @Test
    void calcProjectsAndFilters() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE t (id INTEGER, name VARCHAR)");
                executor.execute("INSERT INTO t VALUES (1, 'a')");
                executor.execute("INSERT INTO t VALUES (2, 'b')");
                executor.execute("INSERT INTO t VALUES (3, 'c')");

                MiniDbScan scan = findScan(new Planner(catalog).plan("SELECT id FROM t"));
                RelDataType inputType = scan.getRowType();
                RelDataType idType = inputType.getFieldList().get(0).getType();

                RexBuilder rb = new RexBuilder(
                        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                RexProgramBuilder pb = new RexProgramBuilder(inputType, rb);
                RexNode idRef = rb.makeInputRef(idType, 0);
                RexNode twice = rb.makeCall(SqlStdOperatorTable.MULTIPLY,
                        idRef, rb.makeExactLiteral(new java.math.BigDecimal(2), idType));
                pb.addProject(twice, "x");
                RexNode gt = rb.makeCall(SqlStdOperatorTable.GREATER_THAN,
                        idRef, rb.makeExactLiteral(new java.math.BigDecimal(1), idType));
                pb.addCondition(gt);
                RexProgram program = pb.getProgram();

                MiniDbCalc calc = new MiniDbCalc(scan.getCluster(),
                        scan.getTraitSet().replace(MiniDbConvention.INSTANCE),
                        scan, program);
                List<Integer> xs = collect(calc, storage, allocator);
                assertEquals(List.of(4, 6), xs);
            } finally {
                storage.close();
            }
        }
    }

    @Test
    void calcWithoutConditionProjectsAllRows() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE t (id INTEGER)");
                executor.execute("INSERT INTO t VALUES (1), (2), (3)");

                MiniDbScan scan = findScan(new Planner(catalog).plan("SELECT id FROM t"));
                RelDataType inputType = scan.getRowType();
                RelDataType idType = inputType.getFieldList().get(0).getType();

                RexBuilder rb = new RexBuilder(
                        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                RexProgramBuilder pb = new RexProgramBuilder(inputType, rb);
                RexNode idRef = rb.makeInputRef(idType, 0);
                pb.addProject(idRef, "x");
                RexProgram program = pb.getProgram();

                MiniDbCalc calc = new MiniDbCalc(scan.getCluster(),
                        scan.getTraitSet().replace(MiniDbConvention.INSTANCE),
                        scan, program);
                List<Integer> xs = collect(calc, storage, allocator);
                assertEquals(List.of(1, 2, 3), xs);
            } finally {
                storage.close();
            }
        }
    }

    @Test
    void calcWithNoMatchingRowsReturnsEmpty() {
        try (BufferAllocator allocator = new RootAllocator()) {
            MiniDbCatalog catalog = new MiniDbCatalog();
            StorageManager storage = new StorageManager(catalog, allocator, dataDir);
            StatsManager stats = new StatsManager(storage);
            QueryExecutor executor = new QueryExecutor(catalog, storage, allocator, stats);
            try {
                executor.execute("CREATE TABLE t (id INTEGER)");
                executor.execute("INSERT INTO t VALUES (1), (2), (3)");

                MiniDbScan scan = findScan(new Planner(catalog).plan("SELECT id FROM t"));
                RelDataType inputType = scan.getRowType();
                RelDataType idType = inputType.getFieldList().get(0).getType();

                RexBuilder rb = new RexBuilder(
                        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                RexProgramBuilder pb = new RexProgramBuilder(inputType, rb);
                RexNode idRef = rb.makeInputRef(idType, 0);
                pb.addProject(idRef, "x");
                RexNode gt = rb.makeCall(SqlStdOperatorTable.GREATER_THAN,
                        idRef, rb.makeExactLiteral(new java.math.BigDecimal(100), idType));
                pb.addCondition(gt);
                RexProgram program = pb.getProgram();

                MiniDbCalc calc = new MiniDbCalc(scan.getCluster(),
                        scan.getTraitSet().replace(MiniDbConvention.INSTANCE),
                        scan, program);
                List<Integer> xs = collect(calc, storage, allocator);
                assertEquals(List.of(), xs);
            } finally {
                storage.close();
            }
        }
    }

    private static MiniDbScan findScan(RelNode node) {
        if (node instanceof MiniDbScan scan) {
            return scan;
        }
        return findScan(node.getInput(0));
    }

    private static List<Integer> collect(MiniDbCalc calc, StorageManager storage,
                                         BufferAllocator allocator) {
        ExecContext ctx = new ExecContext(storage, allocator);
        BatchIterator it = calc.execute(ctx);
        List<Integer> out = new ArrayList<>();
        try {
            while (it.hasNext()) {
                VectorSchemaRoot root = it.next();
                IntVector v = (IntVector) root.getVector("x");
                for (int i = 0; i < root.getRowCount(); i++) {
                    out.add(v.get(i));
                }
                root.close();
            }
        } finally {
            it.close();
        }
        return out;
    }
}
