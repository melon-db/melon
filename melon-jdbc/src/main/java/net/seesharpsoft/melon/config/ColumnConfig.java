package net.seesharpsoft.melon.config;

import net.seesharpsoft.melon.impl.ColumnImpl;

public class ColumnConfig {
    
    public String name;
    
    public ColumnImpl getColumn() {
        ColumnImpl column = new ColumnImpl(null, name);
        return column;
    }
}
