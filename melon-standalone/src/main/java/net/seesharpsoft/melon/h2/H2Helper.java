package net.seesharpsoft.melon.h2;

import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;
import org.h2.tools.SimpleResultSet;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.List;

public class H2Helper {

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
}
