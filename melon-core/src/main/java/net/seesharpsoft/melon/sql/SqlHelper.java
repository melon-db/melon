package net.seesharpsoft.melon.sql;

import net.seesharpsoft.melon.*;

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

    public static String generateInsertStatement(Table table) {
        StringBuilder builder = new StringBuilder("INSERT INTO ")
                .append(sanitizeDbName(table.getName()))
                .append(" (");

        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            builder.append(sanitizeDbName(column.getName()));
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

    public static String generateSelectStatement(Table table) {
        StringBuilder builder = new StringBuilder("SELECT ");

        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            builder.append(sanitizeDbName(column.getName()));
            if (++i == columnLength) {
                builder.append("");
            } else {
                builder.append(",");
            }
        }

        builder.append(" FROM ").append(sanitizeDbName(table.getName()));

        return builder.toString();
    }

    public static String separateEntitiesBySeparator(List<? extends NamedEntity> values, String separator, boolean sanitize) {
        return SqlHelper.separateNameBySeparator(values.stream().map(namedEntity -> namedEntity.getName()).collect(Collectors.toList()), separator, sanitize);
    }
    
    public static String separateNameBySeparator(List<String> values, String separator, boolean sanitize) {
        int valuesSize = values.size();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < valuesSize; ) {
            String value = values.get(i);
            if (sanitize) {
                builder.append(sanitizeDbName(value));
            } else {
                builder.append(value);
            }
            if (++i < valuesSize) {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public static String generateCreateTableStatement(Table table) {
        StringBuilder builder = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(sanitizeDbName(table.getName()))
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
            builder.append(sanitizeDbName(column.getName()));
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
                    .append(separateEntitiesBySeparator(primaryColumns, ",", true))
                    .append(")");
        }
        for (Column referencingColumn : referencingColumns) {
            builder.append(",")
                    .append("FOREIGN KEY (")
                    .append(sanitizeDbName(referencingColumn.getName()))
                    .append(") REFERENCES ")
                    .append(sanitizeDbName(referencingColumn.getReference().getName()))
                    .append("(")
                    .append(SqlHelper.separateNameBySeparator(referencingColumn.getReference().getColumns().stream()
                            .filter(column -> column.isPrimary())
                            .map(column -> column.getName())
                            .collect(Collectors.toList()), ",", true))
                    .append(") ON DELETE SET NULL ON UPDATE CASCADE");
        }

        builder.append(")");

        return builder.toString();
    }

    public static String generateCreateViewStatement(View view) {
        StringBuilder builder = new StringBuilder("CREATE OR REPLACE VIEW ")
                .append(sanitizeDbName(view.getName()))
                .append(" AS ")
                .append(view.getQuery());

        return builder.toString();
    }

    public static String generateClearTableStatement(Table table) {
        StringBuilder builder = new StringBuilder("DELETE FROM ")
                .append(sanitizeDbName(table.getName()));

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

    public static String generateCreateSchemaStatement(Schema schema) {
        StringBuilder builder = new StringBuilder("CREATE SCHEMA IF NOT EXISTS ")
                .append(sanitizeDbName(schema.getName()));
        return builder.toString();
    }
}
