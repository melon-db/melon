package net.seesharpsoft.melon.sql;

import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.View;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqlHelper {

    private static String sanitizeDbName(String name) {
        return name.replaceAll("\\W", "_");
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

    public static String separateValuesBySeparator(List<String> values, String separator, boolean sanitize) {
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

        List<String> primaryColumns = new ArrayList<>();
        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            if (column.isPrimary()) {
                primaryColumns.add(column.getName());
            }
            builder.append(sanitizeDbName(column.getName()));
            if (primaryColumns.contains(column.getName())) {
                builder.append(" VARCHAR(")
                        .append(column.getProperties().getOrDefault("size", 2000))
                        .append(")");
            } else {
                builder.append(" VARCHAR");
            }
            if (++i < columnLength) {
                builder.append(",");
            }
        }
        if (!primaryColumns.isEmpty()) {
            builder.append(",")
                    .append("PRIMARY KEY (")
                    .append(separateValuesBySeparator(primaryColumns, ",", true))
                    .append(")");
        }
        builder.append(")");

        return builder.toString();
    }

    public static String generateCreateViewStatement(View view) {
        StringBuilder builder = new StringBuilder("CREATE VIEW IF NOT EXISTS ")
                .append(sanitizeDbName(view.getName()))
                .append(" AS ")
                .append(view.getQuery());

        return builder.toString();
    }

    public static String generateClearTableStatement(Table table) {
        StringBuilder builder = new StringBuilder("TRUNCATE TABLE ")
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
}
