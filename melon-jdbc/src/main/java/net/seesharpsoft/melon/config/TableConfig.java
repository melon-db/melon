package net.seesharpsoft.melon.config;

import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.ColumnImpl;
import net.seesharpsoft.melon.impl.TableImpl;

import java.util.List;
import java.util.Properties;

public class TableConfig {
    
    public String name;
    
    public StorageConfig storage;
    
    public List<ColumnConfig> columns;
    
    public Table getTable(Properties properties) {
        TableImpl table = new TableImpl(this.name);
        table.setStorage(storage.getStorage(table, properties));
        columns.forEach(columnConfig -> {
            ColumnImpl column = columnConfig.getColumn();
            column.setTable(table);
            table.addColumn(column);
        });
        return table;
    }
}
