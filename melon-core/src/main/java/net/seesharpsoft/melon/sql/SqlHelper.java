package net.seesharpsoft.melon.sql;

import net.seesharpsoft.melon.*;
import net.seesharpsoft.melon.Schema;
import net.seesharpsoft.melon.Table;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SqlHelper {

    private SqlHelper() {
        // static
    }

    public static String generateMergeStatement(Connection connection, Table table) {
        try (DSLContext context = DSL.using(connection)) {
            return context.mergeInto(DSL.table(DSL.name(table.getName())))
                    .columns(table.getColumns().stream().map(column -> DSL.field(DSL.name(column.getName()))).collect(Collectors.toList()))
                    .values(table.getColumns().stream().map(column -> null).collect(Collectors.toList()))
                    .getSQL(ParamType.INDEXED);
        }
    }

    public static String generateInsertStatement(Connection connection, Table table) {
        try (DSLContext context = DSL.using(connection)) {
            return context.insertInto(DSL.table(table.getName()))
                    .columns(table.getColumns().stream().map(column -> DSL.field(column.getName())).collect(Collectors.toList()))
                    .values(table.getColumns().stream().map(column -> null).collect(Collectors.toList()))
                    .getSQL(ParamType.INDEXED);
        }
    }

    public static String generateSelectStatement(Connection connection, Table table) {
        try (DSLContext context = DSL.using(connection)) {
            SelectJoinStep selectStep = context
                    .select(table.getColumns().stream().map(column -> DSL.field(DSL.name(column.getName()))).collect(Collectors.toList()))
                    .from(DSL.table(table.getName()).getQualifiedName());
            String order = table.getStorage().getProperties().get(Storage.PROPERTY_STORAGE_RECORD_ORDER);
            if (order != null && !order.isEmpty()) {
                return selectStep.orderBy(Arrays.stream(order.split(",")).map(field -> DSL.field(DSL.name(field.trim()))).collect(Collectors.toList())).getSQL();
            }
            return selectStep.getSQL();
        }
    }

    public static String generateCreateTableStatement(Connection connection, Table table) {
        try (DSLContext context = DSL.using(connection)) {
            return context.createTableIfNotExists(table.getName())
                    .columns(table.getColumns().stream()
                            .map(column -> column.getProperties().containsKey(Column.PROPERTY_LENGTH) ?
                                    DSL.field(DSL.name(column.getName()), SQLDataType.VARCHAR.length(column.getProperties().getOrDefault(Column.PROPERTY_LENGTH, Column.DEFAULT_LENGTH))) :
                                    DSL.field(DSL.name(column.getName()), SQLDataType.VARCHAR.length(4000)))
                            .collect(Collectors.toList()))
                    .constraints(table.getPrimaryColumn() != null ? Collections.singleton(DSL.primaryKey(table.getPrimaryColumn().getName())) : Collections.emptySet())
                    .constraints(table.getReferenceColumns().stream()
                            .map(column -> DSL.foreignKey(column.getName()).references(column.getReference().getName()).onDeleteSetNull().onUpdateCascade())
                            .collect(Collectors.toList()))
                    .getSQL();
        }
    }

    public static String generateCreateViewStatement(Connection connection, View view) {
        try (DSLContext context = DSL.using(connection)) {
            Queries queries = context.parser().parse(view.getQuery());
            Select select = (Select) queries.iterator().next();
            return context.createOrReplaceView(DSL.table(view.getName()))
                    .as(select)
                    .getSQL();
        }
    }

    public static String generateClearTableStatement(Connection connection, Table table, List<String> primaryValuesToKeep) {
        try (DSLContext context = DSL.using(connection)) {
            DeleteWhereStep deleteWhereStep = context.deleteFrom(DSL.table(DSL.name(table.getName())));
            if (primaryValuesToKeep.isEmpty()) {
                return deleteWhereStep.getSQL();
            }
            return deleteWhereStep.where(DSL.not(DSL.field(DSL.name(table.getPrimaryColumn().getName())).in(primaryValuesToKeep)))
                    .getSQL();
        }
    }

    public static List<List<String>> fromResultSet(ResultSet rs) {
        List<List<String>> recordList = new ArrayList<>();
        try {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                List<String> values = new ArrayList<>();
                for (int i = 1; i <= meta.getColumnCount(); ++i) {
                    values.add(rs.getString(i));
                }
                recordList.add(values);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return recordList;
    }

    public static String generateCreateSchemaStatement(Connection connection, Schema schema) {
        try (DSLContext context = DSL.using(connection)) {
            return context.createSchemaIfNotExists(schema.getName()).getSQL();
        }
    }

    public static String generateSetSchemaStatement(Connection connection, Schema schema) {
        try (DSLContext context = DSL.using(connection)) {
            return context.setSchema(schema.getName()).getSQL();
        }
    }

}
