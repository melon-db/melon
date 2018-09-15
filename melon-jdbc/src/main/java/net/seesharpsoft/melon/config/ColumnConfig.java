package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.ColumnImpl;

public class ColumnConfig {
    
    public Properties properties;
    
    public String name;
    
    public ColumnImpl getColumn(Table table, Properties additionalProperties) {
        Properties finalProperties = new Properties(additionalProperties);
        finalProperties.putAll(properties);
        
        ColumnImpl column = new ColumnImpl(table, name, finalProperties);
        return column;
    }
}
