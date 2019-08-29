package net.seesharpsoft.melon.sql;

import net.seesharpsoft.melon.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlHelper {

    public static String sanitizeDbName(String name) {
        return name.replaceAll("^[\\d\\W]*", "").replaceAll("\\W", "_").toUpperCase();
    }

    public static String generateInsertOrMergeStatement(Connection connection, Table table, String insertOrMerge) {
        StringBuilder builder = new StringBuilder(insertOrMerge)
                .append(" INTO ")
                .append(table.getFormattedName())
                .append(" (");

        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            builder.append(column.getFormattedName());
            if (++i == columnLength) {
                builder.append(")");
            } else {
                builder.append(",");
            }
        }

        builder.append(" VALUES (");
        for (int i = 0; i < columnLength; ) {
            builder.append("?");
            if (++i == columnLength) {
                builder.append(")");
            } else {
                builder.append(",");
            }
        }

        return builder.toString();
    }

    public static String generateMergeStatement(Connection connection, Table table) {
        return generateInsertOrMergeStatement(connection, table, "MERGE");
    }

    public static String generateInsertStatement(Connection connection, Table table) {
        return generateInsertOrMergeStatement(connection, table, "INSERT");
    }

    public static String generateSelectStatement(Connection connection, Table table) {
        StringBuilder builder = new StringBuilder("SELECT ");

        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            builder.append(column.getFormattedName());
            if (++i == columnLength) {
                builder.append("");
            } else {
                builder.append(",");
            }
        }

        builder.append(" FROM ").append(table.getFormattedName());

        String order = table.getStorage().getProperties().get(Storage.PROPERTY_STORAGE_RECORD_ORDER);
        if (order != null && !order.isEmpty()) {
            builder.append(" ORDER BY ")
                    .append(order);
        }

        return builder.toString();
    }

    public static String separateEntitiesBySeparator(List<? extends NamedEntity> values, String separator) {
        return SqlHelper.separateNameBySeparator(values.stream().map(namedEntity -> namedEntity.getFormattedName()).collect(Collectors.toList()), separator);
    }

    public static String separateNameBySeparator(List<String> values, String separator) {
        int valuesSize = values.size();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < valuesSize; ) {
            builder.append(values.get(i));
            if (++i < valuesSize) {
                builder.append(separator);
            }
        }
        return builder.toString();
    }

    public static String generateCreateTableStatement(Connection connection, Table table) {
        StringBuilder builder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(table.getFormattedName())
                .append(" (");

        List<Column> primaryColumns = new ArrayList<>();
        List<Column> referencingColumns = new ArrayList<>();
        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            if (column.isPrimary()) {
                primaryColumns.add(column);
            }
            if (column.getReference() != null) {
                referencingColumns.add(column);
            }
            builder.append(column.getFormattedName());
            builder.append(" VARCHAR");
            if (column.getProperties().containsKey(Column.PROPERTY_LENGTH)) {
                builder.append("(")
                        .append(column.getProperties().getOrDefault(Column.PROPERTY_LENGTH, Column.DEFAULT_LENGTH))
                        .append(")");
            }

            if (++i < columnLength) {
                builder.append(",");
            }
        }
        if (!primaryColumns.isEmpty()) {
            builder.append(",")
                    .append("PRIMARY KEY (")
                    .append(separateEntitiesBySeparator(primaryColumns, ","))
                    .append(")");
        }
        for (Column referencingColumn : referencingColumns) {
            builder.append(",")
                    .append("FOREIGN KEY (")
                    .append(referencingColumn.getFormattedName())
                    .append(") REFERENCES ")
                    .append(referencingColumn.getReference().getFormattedName())
                    .append("(")
                    .append(SqlHelper.separateNameBySeparator(referencingColumn.getReference().getColumns().stream()
                            .filter(column -> column.isPrimary())
                            .map(column -> column.getFormattedName())
                            .collect(Collectors.toList()), ","))
                    .append(") ON DELETE SET NULL ON UPDATE CASCADE");
        }

        builder.append(")");

        return builder.toString();
    }

    public static String generateCreateViewStatement(Connection connection, View view) {
        StringBuilder builder = new StringBuilder("CREATE OR REPLACE VIEW ")
                .append(view.getFormattedName())
                .append(" AS ")
                .append(view.getQuery());

        return builder.toString();
    }

    public static String generateClearTableStatement(Connection connection, Table table, List<String> primaryValuesToKeep) {
        StringBuilder builder = new StringBuilder("DELETE FROM ")
                .append(table.getFormattedName());

        if (primaryValuesToKeep != null && !primaryValuesToKeep.isEmpty()) {
            builder.append(" WHERE NOT ")
                    .append(table.getPrimaryColumn().getFormattedName())
                    .append(" IN (")
                    .append(separateNameBySeparator(primaryValuesToKeep.stream().map(value -> "?").collect(Collectors.toList()), ","))
                    .append(")");
        }

        return builder.toString();
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

    private SqlHelper() {
        // static
    }

    public static String generateCreateSchemaStatement(Connection connection, Schema schema) {
        StringBuilder builder = new StringBuilder("CREATE SCHEMA IF NOT EXISTS ")
                .append(schema.getFormattedName());
        return builder.toString();
    }

    public static String generateSetSchemaStatement(Connection connection, Schema schema) {
        StringBuilder builder = new StringBuilder("SET SCHEMA ")
                .append(schema.getFormattedName());
        return builder.toString();
    }
}
