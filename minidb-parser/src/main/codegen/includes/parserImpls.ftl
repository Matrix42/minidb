<#--
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
-->

boolean IfNotExistsOpt() :
{
}
{
    <IF> <NOT> <EXISTS> { return true; }
|
    { return false; }
}

boolean IfExistsOpt() :
{
}
{
    <IF> <EXISTS> { return true; }
|
    { return false; }
}

SqlCreate SqlCreateSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
}
{
    <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    {
        return SqlDdlNodes.createSchema(s.end(this), replace, ifNotExists, id);
    }
}

SqlCreate SqlCreateForeignSchema(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNode type = null;
    SqlNode library = null;
    SqlNodeList optionList = null;
}
{
    <FOREIGN> <SCHEMA> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
         <TYPE> type = StringLiteral()
    |
         <LIBRARY> library = StringLiteral()
    )
    [ optionList = Options() ]
    {
        return SqlDdlNodes.createForeignSchema(s.end(this), replace,
            ifNotExists, id, type, library, optionList);
    }
}

SqlNodeList Options() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <OPTIONS> { s = span(); } <LPAREN>
    [
        Option(list)
        (
            <COMMA>
            Option(list)
        )*
    ]
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void Option(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlNode value;
}
{
    id = SimpleIdentifier()
    value = Literal() {
        list.add(id);
        list.add(value);
    }
}

SqlNodeList TableElementList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    TableElement(list)
    (
        <COMMA> TableElement(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void TableElement(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    final SqlNode e;
    SqlIdentifier name = null;
    final SqlNodeList columnList;
    final Span s = Span.of();
    final ColumnStrategy strategy;
    SqlIdentifier refTable = null;
    SqlNodeList refColumns = null;
}
{
    LOOKAHEAD(2) id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
        (
            [ <GENERATED> <ALWAYS> ] <AS> <LPAREN>
            e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN>
            (
                <VIRTUAL> { strategy = ColumnStrategy.VIRTUAL; }
            |
                <STORED> { strategy = ColumnStrategy.STORED; }
            |
                { strategy = ColumnStrategy.VIRTUAL; }
            )
        |
            <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) {
                strategy = ColumnStrategy.DEFAULT;
            }
        |
            {
                e = null;
                strategy = nullable ? ColumnStrategy.NULLABLE
                    : ColumnStrategy.NOT_NULLABLE;
            }
        )
        (
            <PRIMARY> <KEY> {
                list.add(SqlDdlNodes.primary(s.add(id).end(this), null, SqlNodeList.of(id)));
            }
        |
            <UNIQUE> {
                list.add(SqlDdlNodes.unique(s.add(id).end(this), null, SqlNodeList.of(id)));
            }
        |
            <REFERENCES> refTable = CompoundIdentifier()
            [ refColumns = ParenthesizedSimpleIdentifierList() ]
            {
                list.add(new SqlForeignKeyConstraint(s.add(id).end(this), null,
                    SqlNodeList.of(id), refTable, refColumns));
            }
        )?
        {
            list.add(
                SqlDdlNodes.column(s.add(id).end(this), id,
                    type.withNullable(nullable), e, strategy));
        }
    |
        { list.add(id); }
    )
|
    id = SimpleIdentifier() {
        list.add(id);
    }
|
    [ <CONSTRAINT> { s.add(this); } name = SimpleIdentifier() ]
    (
        <CHECK> { s.add(this); } <LPAREN>
        e = Expression(ExprContext.ACCEPT_SUB_QUERY) <RPAREN> {
            list.add(SqlDdlNodes.check(s.end(this), name, e));
        }
    |
        <UNIQUE> { s.add(this); }
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.unique(s.end(columnList), name, columnList));
        }
    |
        <PRIMARY>  { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList() {
            list.add(SqlDdlNodes.primary(s.end(columnList), name, columnList));
        }
    |
        <FOREIGN> { s.add(this); } <KEY>
        columnList = ParenthesizedSimpleIdentifierList()
        <REFERENCES> refTable = CompoundIdentifier()
        [ refColumns = ParenthesizedSimpleIdentifierList() ]
        {
            list.add(new SqlForeignKeyConstraint(s.end(this), name, columnList, refTable, refColumns));
        }
    )
}

SqlNodeList AttributeDefList() :
{
    final Span s;
    final List<SqlNode> list = new ArrayList<SqlNode>();
}
{
    <LPAREN> { s = span(); }
    AttributeDef(list)
    (
        <COMMA> AttributeDef(list)
    )*
    <RPAREN> {
        return new SqlNodeList(list, s.end(this));
    }
}

void AttributeDef(List<SqlNode> list) :
{
    final SqlIdentifier id;
    final SqlDataTypeSpec type;
    final boolean nullable;
    SqlNode e = null;
    final Span s = Span.of();
}
{
    id = SimpleIdentifier()
    (
        type = DataType()
        nullable = NullableOptDefaultTrue()
    )
    [ <DEFAULT_> e = Expression(ExprContext.ACCEPT_SUB_QUERY) ]
    {
        list.add(SqlDdlNodes.attribute(s.add(id).end(this), id,
            type.withNullable(nullable), e, null));
    }
}

SqlCreate SqlCreateType(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList attributeDefList = null;
    SqlDataTypeSpec type = null;
}
{
    <TYPE>
    id = CompoundIdentifier()
    <AS>
    (
        attributeDefList = AttributeDefList()
    |
        type = DataType()
    )
    {
        return SqlDdlNodes.createType(s.end(this), replace, id, attributeDefList, type);
    }
}

SqlCreate SqlCreateTable(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList tableElementList = null;
    SqlNode query = null;
    SqlTableOptions tableOptions = null;

    SqlCreate createTableLike = null;
}
{
    <TABLE> ifNotExists = IfNotExistsOpt() id = CompoundIdentifier()
    (
        <LIKE> createTableLike = SqlCreateTableLike(s, replace, ifNotExists, id) {
            return createTableLike;
        }
    |
        [ tableElementList = TableElementList() ]
        [ <WITH> tableOptions = TableOptions() {
            if (tableElementList == null) {
                tableElementList = new SqlNodeList(getPos());
            }
            tableElementList.add(tableOptions);
        } ]
        [ <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) ]
        {
            return SqlDdlNodes.createTable(s.end(this), replace, ifNotExists, id, tableElementList, query);
        }
    )
}

/**
 * Parses a Flink-style {@code WITH (key = value, ...)} table option list.
 * key is a string literal, value is a string/boolean/numeric literal.
 */
SqlTableOptions TableOptions() :
{
    final List<SqlNode> entries = new ArrayList<SqlNode>();
    SqlNode key;
    SqlNode value;
    final Span s;
}
{
    { s = span(); }
    <LPAREN>
    key = StringLiteral() <EQ> value = TableOptionValue()
    { entries.add(key); entries.add(value); }
    ( <COMMA> key = StringLiteral() <EQ> value = TableOptionValue()
      { entries.add(key); entries.add(value); } )*
    <RPAREN> {
        return new SqlTableOptions(s.end(this), entries);
    }
}

/** Table option value: string/boolean/numeric literal. */
SqlNode TableOptionValue() :
{
    SqlNode value;
}
{
    value = StringLiteral() { return value; }
|
    value = NumericLiteral() { return value; }
|
    <TRUE> { return SqlLiteral.createBoolean(true, getPos()); }
|
    <FALSE> { return SqlLiteral.createBoolean(false, getPos()); }
}

SqlCreate SqlCreateTableLike(Span s, boolean replace, boolean ifNotExists, SqlIdentifier id) :
{
    final SqlIdentifier sourceTable;
    final boolean likeOptions;
    final SqlNodeList including = new SqlNodeList(getPos());
    final SqlNodeList excluding = new SqlNodeList(getPos());
}
{
    sourceTable = CompoundIdentifier()
    [ LikeOptions(including, excluding) ]
    {
        return SqlDdlNodes.createTableLike(s.end(this), replace, ifNotExists, id, sourceTable, including, excluding);
    }
}

void LikeOptions(SqlNodeList including, SqlNodeList excluding) :
{
}
{
    LikeOption(including, excluding)
    (
        LikeOption(including, excluding)
    )*
}

void LikeOption(SqlNodeList includingOptions, SqlNodeList excludingOptions) :
{
    boolean including = false;
    SqlCreateTableLike.LikeOption option;
}
{
    (
        <INCLUDING> { including = true; }
    |
        <EXCLUDING> { including = false; }
    )
    (
        <ALL> { option = SqlCreateTableLike.LikeOption.ALL; }
    |
        <DEFAULTS> { option = SqlCreateTableLike.LikeOption.DEFAULTS; }
    |
        <GENERATED> { option = SqlCreateTableLike.LikeOption.GENERATED; }
    )
    {
        if (including) {
            includingOptions.add(option.symbol(getPos()));
        } else {
            excludingOptions.add(option.symbol(getPos()));
        }
    }
}

SqlCreate SqlCreateView(Span s, boolean replace) :
{
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <VIEW> id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createView(s.end(this), replace, id, columnList,
            query);
    }
}

SqlCreate SqlCreateMaterializedView(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    SqlNodeList columnList = null;
    final SqlNode query;
}
{
    <MATERIALIZED> <VIEW> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    [ columnList = ParenthesizedSimpleIdentifierList() ]
    <AS> query = OrderedQueryOrExpr(ExprContext.ACCEPT_QUERY) {
        return SqlDdlNodes.createMaterializedView(s.end(this), replace,
            ifNotExists, id, columnList, query);
    }
}

private void FunctionJarDef(SqlNodeList usingList) :
{
    final SqlDdlNodes.FileType fileType;
    final SqlNode uri;
}
{
    (
        <ARCHIVE> { fileType = SqlDdlNodes.FileType.ARCHIVE; }
    |
        <FILE> { fileType = SqlDdlNodes.FileType.FILE; }
    |
        <JAR> { fileType = SqlDdlNodes.FileType.JAR; }
    ) {
        usingList.add(SqlLiteral.createSymbol(fileType, getPos()));
    }
    uri = StringLiteral() {
        usingList.add(uri);
    }
}

SqlCreate SqlCreateFunction(Span s, boolean replace) :
{
    final boolean ifNotExists;
    final SqlIdentifier id;
    final SqlNode className;
    SqlNodeList usingList = SqlNodeList.EMPTY;
}
{
    <FUNCTION> ifNotExists = IfNotExistsOpt()
    id = CompoundIdentifier()
    <AS>
    className = StringLiteral()
    [
        <USING> {
            usingList = new SqlNodeList(getPos());
        }
        FunctionJarDef(usingList)
        (
            <COMMA>
            FunctionJarDef(usingList)
        )*
    ] {
        return SqlDdlNodes.createFunction(s.end(this), replace, ifNotExists,
            id, className, usingList);
    }
}

SqlDrop SqlDropSchema(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
    final boolean foreign;
}
{
    (
        <FOREIGN> { foreign = true; }
    |
        { foreign = false; }
    )
    <SCHEMA> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropSchema(s.end(this), foreign, ifExists, id);
    }
}

SqlDrop SqlDropType(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TYPE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropType(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropTable(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <TABLE> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropTable(s.end(this), ifExists, id);
    }
}

SqlTruncate SqlTruncateTable(Span s) :
{
    final SqlIdentifier id;
    final boolean continueIdentity;
}
{
      <TABLE> id = CompoundIdentifier()
    (
      <CONTINUE> <IDENTITY> { continueIdentity = true; }
      |
      <RESTART> <IDENTITY> { continueIdentity = false; }
      |
      { continueIdentity = true; }
    )
    {
        return SqlDdlNodes.truncateTable(s.end(this), id, continueIdentity);
    }
}

SqlDrop SqlDropView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropMaterializedView(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <MATERIALIZED> <VIEW> ifExists = IfExistsOpt() id = CompoundIdentifier() {
        return SqlDdlNodes.dropMaterializedView(s.end(this), ifExists, id);
    }
}

SqlDrop SqlDropFunction(Span s, boolean replace) :
{
    final boolean ifExists;
    final SqlIdentifier id;
}
{
    <FUNCTION> ifExists = IfExistsOpt()
    id = CompoundIdentifier() {
        return SqlDdlNodes.dropFunction(s.end(this), ifExists, id);
    }
}

/**
 * Parses an {@code ALTER TABLE} statement. One statement carries exactly one
 * operation (add/drop column, rename, alter type/not-null, add/drop constraint).
 */
SqlNode SqlAlterTable() :
{
    final Span s;
    SqlIdentifier table;
    SqlIdentifier column = null;
    SqlIdentifier newColumn = null;
    SqlIdentifier newTable = null;
    SqlIdentifier constraintName = null;
    SqlIdentifier refTable = null;
    SqlDataTypeSpec dataType = null;
    SqlNode defaultExpr = null;
    SqlNodeList columns = null;
    SqlNodeList refColumns = null;
    Boolean nullable = null;
    SqlKind constraintKind = null;
    SqlAlterTable.AlterKind kind;
}
{
    <ALTER> { s = span(); } <TABLE> table = CompoundIdentifier()
    (
        <ADD>
        (
            LOOKAHEAD(2) <COLUMN> column = SimpleIdentifier() dataType = DataType()
                nullable = NullableOptDefaultTrue()
                [ <DEFAULT_> defaultExpr = Literal() ]
                { kind = SqlAlterTable.AlterKind.ADD_COLUMN; }
        |
            LOOKAHEAD(2) <CONSTRAINT> constraintName = SimpleIdentifier()
            (
                <PRIMARY> <KEY> columns = ParenthesizedSimpleIdentifierList()
                    { constraintKind = SqlKind.PRIMARY_KEY; }
            |
                <UNIQUE> columns = ParenthesizedSimpleIdentifierList()
                    { constraintKind = SqlKind.UNIQUE; }
            |
                <FOREIGN> <KEY> columns = ParenthesizedSimpleIdentifierList()
                    <REFERENCES> refTable = CompoundIdentifier()
                    [ refColumns = ParenthesizedSimpleIdentifierList() ]
                    { constraintKind = SqlKind.OTHER; }
            )
            { kind = SqlAlterTable.AlterKind.ADD_CONSTRAINT; }
        |
            LOOKAHEAD(2) <PRIMARY> <KEY> columns = ParenthesizedSimpleIdentifierList()
                { kind = SqlAlterTable.AlterKind.ADD_CONSTRAINT; constraintKind = SqlKind.PRIMARY_KEY; }
        |
            LOOKAHEAD(2) <UNIQUE> columns = ParenthesizedSimpleIdentifierList()
                { kind = SqlAlterTable.AlterKind.ADD_CONSTRAINT; constraintKind = SqlKind.UNIQUE; }
        |
            <FOREIGN> <KEY> columns = ParenthesizedSimpleIdentifierList()
                <REFERENCES> refTable = CompoundIdentifier()
                [ refColumns = ParenthesizedSimpleIdentifierList() ]
                { kind = SqlAlterTable.AlterKind.ADD_CONSTRAINT; constraintKind = SqlKind.OTHER; }
        |
            column = SimpleIdentifier() dataType = DataType()
                nullable = NullableOptDefaultTrue()
                [ <DEFAULT_> defaultExpr = Literal() ]
                { kind = SqlAlterTable.AlterKind.ADD_COLUMN; }
        )
    |
        <DROP>
        (
            LOOKAHEAD(2) <COLUMN> column = SimpleIdentifier()
                { kind = SqlAlterTable.AlterKind.DROP_COLUMN; }
        |
            LOOKAHEAD(2) <CONSTRAINT> constraintName = SimpleIdentifier()
                { kind = SqlAlterTable.AlterKind.DROP_CONSTRAINT; }
        |
            <PRIMARY> <KEY>
                { kind = SqlAlterTable.AlterKind.DROP_CONSTRAINT; constraintKind = SqlKind.PRIMARY_KEY; }
        |
            column = SimpleIdentifier()
                { kind = SqlAlterTable.AlterKind.DROP_COLUMN; }
        )
    |
        <RENAME>
        (
            LOOKAHEAD(2) <TO> newTable = CompoundIdentifier()
                { kind = SqlAlterTable.AlterKind.RENAME_TABLE; }
        |
            LOOKAHEAD(2) <COLUMN> column = SimpleIdentifier() <TO> newColumn = SimpleIdentifier()
                { kind = SqlAlterTable.AlterKind.RENAME_COLUMN; }
        |
            column = SimpleIdentifier() <TO> newColumn = SimpleIdentifier()
                { kind = SqlAlterTable.AlterKind.RENAME_COLUMN; }
        )
    |
        <ALTER>
        (
            LOOKAHEAD(2) <COLUMN> column = SimpleIdentifier()
            (
                <SET>
                (
                    <DATA> <TYPE> dataType = DataType()
                        { kind = SqlAlterTable.AlterKind.ALTER_TYPE; }
                |
                    <NOT> <NULL>
                        { kind = SqlAlterTable.AlterKind.SET_NOT_NULL; nullable = false; }
                )
            |
                <DROP> <NOT> <NULL>
                    { kind = SqlAlterTable.AlterKind.DROP_NOT_NULL; nullable = true; }
            )
        |
            column = SimpleIdentifier()
            (
                <SET>
                (
                    <DATA> <TYPE> dataType = DataType()
                        { kind = SqlAlterTable.AlterKind.ALTER_TYPE; }
                |
                    <NOT> <NULL>
                        { kind = SqlAlterTable.AlterKind.SET_NOT_NULL; nullable = false; }
                )
            |
                <DROP> <NOT> <NULL>
                    { kind = SqlAlterTable.AlterKind.DROP_NOT_NULL; nullable = true; }
            )
        )
    )
    {
        return new SqlAlterTable(s.end(this), kind, table, column, newColumn, newTable,
                dataType, defaultExpr, nullable, constraintName, constraintKind,
                columns, refTable, refColumns);
    }
}

