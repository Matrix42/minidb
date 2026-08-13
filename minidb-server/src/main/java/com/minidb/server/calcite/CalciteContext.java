package com.minidb.server.calcite;

import com.minidb.server.catalog.MiniDbCatalog;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
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
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
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
                .withQuoting(Quoting.DOUBLE_QUOTE)
                .withUnquotedCasing(Casing.UNCHANGED)
                .withCaseSensitive(false);
    }

    private SchemaPlus createRootSchema(String currentSchema) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        rootSchema.add(SCHEMA_NAME,
                new MiniDbRootCalciteSchema(catalog, currentSchema));
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
        return plan(sql, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public RelRoot plan(String sql, String currentSchema) {
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        SqlTypeFactoryImpl typeFactory =
                new Utf8SqlTypeFactory(RelDataTypeSystem.DEFAULT);
        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        return planInCluster(sql, cluster, currentSchema);
    }

    public RelRoot planInCluster(String sql, RelOptCluster cluster) {
        return planInCluster(sql, cluster, MiniDbCatalog.DEFAULT_SCHEMA);
    }

    public RelRoot planInCluster(String sql, RelOptCluster cluster, String currentSchema) {
        SqlNode parsed = parse(sql);
        SqlTypeFactoryImpl typeFactory =
                (SqlTypeFactoryImpl) cluster.getTypeFactory();
        CalciteCatalogReader catalogReader = buildCatalogReader(typeFactory, currentSchema);
        // 标准算子(STANDARD)+ MYSQL/POSTGRESQL 库算子(提供 CONCAT(arg,...)/LENGTH 等常用函数)。
        // 注意:getOperatorTable 只返回显式列出的库;漏 STANDARD 会丢掉 +/> 等标准算子。
        SqlValidator validator = SqlValidatorUtil.newValidator(
                SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                        SqlLibrary.STANDARD, SqlLibrary.MYSQL, SqlLibrary.POSTGRESQL),
                catalogReader, typeFactory,
                SqlValidator.Config.DEFAULT.withIdentifierExpansion(true));
        SqlNode validated = validator.validate(parsed);
        SqlToRelConverter converter = new SqlToRelConverter(
                null, validator, catalogReader, cluster,
                StandardConvertletTable.INSTANCE,
                SqlToRelConverter.config());
        return converter.convertQuery(validated, false, true);
    }

    private CalciteCatalogReader buildCatalogReader(
            SqlTypeFactoryImpl typeFactory, String currentSchema) {
        return new CalciteCatalogReader(
                CalciteSchema.from(createRootSchema(currentSchema)),
                List.of(SCHEMA_NAME),
                typeFactory,
                new CalciteConnectionConfigImpl(new Properties()));
    }
}
