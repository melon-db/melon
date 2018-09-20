package net.seesharpsoft.melon.impl;

import lombok.Getter;
import lombok.Setter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;

public class ColumnImpl implements Column {
    
    @Getter
    protected final Properties properties;
    
    @Getter
    protected final Table table;
    
    @Getter
    protected final String name;
    
    @Getter
    @Setter
    protected boolean primary = false;
    
    @Setter
    @Getter
    protected Table reference;
    
    public ColumnImpl(Table table, String name, Properties properties) {
        this.table = table;
        this.name = name;
        this.properties = new Properties(properties);
    }
}
