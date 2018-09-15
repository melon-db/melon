package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.ColumnImpl;
import net.seesharpsoft.melon.impl.TableImpl;

import java.util.List;

public class TableConfig {
    
    public String name;
    
    public StorageConfig storage;
    
    public List<ColumnConfig> columns;
    
    public Properties properties;
    
    public Table getTable(Properties additionalProperties) {
        Properties finalProperties = new Properties(additionalProperties);
        finalProperties.putAll(properties);
        
        TableImpl table = new TableImpl(this.name, finalProperties);
        
        table.setStorage(storage.getStorage(table, finalProperties));
        columns.forEach(columnConfig -> {
            ColumnImpl column = columnConfig.getColumn(table, finalProperties);
            table.addColumn(column);
        });
        return table;
    }
}
