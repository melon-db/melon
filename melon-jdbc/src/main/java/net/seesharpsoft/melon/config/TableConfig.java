package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.ColumnImpl;
import net.seesharpsoft.melon.impl.TableImpl;

import java.util.List;

public class TableConfig extends ConfigBase {
    
    public String name;
    
    public StorageConfig storage;
    
    public List<ColumnConfig> columns;
    
    public Table getTable(Properties additionalProperties) {
        Properties finalProperties = getProperties(additionalProperties);
        
        TableImpl table = new TableImpl(this.name, finalProperties);
        
        table.setStorage(storage.getStorage(table, finalProperties));
        if (columns != null) {
            columns.forEach(columnConfig -> {
                ColumnImpl column = columnConfig.getColumn(table, finalProperties);
                table.addColumn(column);
            });
        }
        return table;
    }
}
