package net.seesharpsoft.melon.sql;

import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;
import org.h2.tools.SimpleResultSet;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class SqlHelper {

    public static String generateInsertStatement(Table table) {
        StringBuilder builder = new StringBuilder("INSERT INTO ")
                .append(table.getName())
                .append(" (");

        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            builder.append(column.getName());
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
            builder.append(column.getName());
            if (++i == columnLength) {
                builder.append("");
            } else {
                builder.append(",");
            }
        }

        builder.append(" FROM ").append(table.getName());

        return builder.toString();
    }

    public static String generateCreateTableStatement(Table table) {
        StringBuilder builder = new StringBuilder("CREATE TABLE ")
                .append(table.getName())
                .append(" (");

        int columnLength = table.getColumns().size();
        for (int i = 0; i < columnLength; ) {
            Column column = table.getColumns().get(i);
            builder.append(column.getName()).append(" VARCHAR");
            if (++i == columnLength) {
                builder.append(")");
            } else {
                builder.append(",");
            }
        }
        return builder.toString();
    }

    public static String generateClearTableStatement(Table table) {
        StringBuilder builder = new StringBuilder("TRUNCATE TABLE ")
                .append(table.getName());

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

    public static ResultSet toResultSet(Table table, List<List<String>> records) {
        SimpleResultSet rs = new SimpleResultSet();
        for (Column column : table.getColumns()) {
            rs.addColumn(column.getName(), Types.VARCHAR, 0, 0);
        }
        for (List<String> record : records) {
            rs.addRow(record.toArray());
        }
        return rs;
    }
    
    private SqlHelper() {
        // static
    }
}
