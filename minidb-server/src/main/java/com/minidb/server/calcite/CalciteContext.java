package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.tools.Frameworks;

public class CalciteContext {

    public static final String SCHEMA_NAME = "minidb";

    private final MiniDbCatalog catalog;
    private final SqlParser.Config parserConfig;

    public CalciteContext(MiniDbCatalog catalog) {
        this.catalog = catalog;
        this.parserConfig = SqlParser.config()
                .withParserFactory(SqlDdlParserImpl.FACTORY)
                .withLex(Lex.MYSQL)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(false);
    }

    private SchemaPlus createRootSchema() {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add(SCHEMA_NAME, new MiniDbCalciteSchema(catalog));
        return rootSchema;
    }

    public SqlNode parse(String sql) {
        try {
            return SqlParser.create(sql, parserConfig).parseStmt();
        } catch (SqlParseException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    public RelRoot plan(String sql) {
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        SqlTypeFactoryImpl typeFactory =
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        return planInCluster(sql, cluster);
    }

    public RelRoot planInCluster(String sql, RelOptCluster cluster) {
        SqlNode parsed = parse(sql);
        SqlTypeFactoryImpl typeFactory =
                (SqlTypeFactoryImpl) cluster.getTypeFactory();
        CalciteCatalogReader catalogReader = buildCatalogReader(typeFactory);
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlStdOperatorTable.instance(), catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validated = validator.validate(parsed);
        SqlToRelConverter converter = new SqlToRelConverter(
                null, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());
        return converter.convertQuery(validated, false, true);
    }

    private CalciteCatalogReader buildCatalogReader(SqlTypeFactoryImpl typeFactory) {
        return new CalciteCatalogReader(
                CalciteSchema.from(createRootSchema()),
                List.of(SCHEMA_NAME),
                typeFactory,
                new CalciteConnectionConfigImpl(new Properties()));
    }
}
