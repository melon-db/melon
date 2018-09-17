package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.ColumnImpl;

public class ColumnConfig extends ConfigBase {
    
    public String name;
    
    public boolean primary = false;
    
    public ColumnImpl getColumn(Table table, Properties additionalProperties) {
        ColumnImpl column = new ColumnImpl(table, name, getProperties(additionalProperties));
        column.setPrimary(primary);
        return column;
    }
}
