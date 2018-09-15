package net.seesharpsoft.melon.impl;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;

@NoArgsConstructor
public class ColumnImpl implements Column {
    
    @Setter
    @Getter
    private Table table;
    
    @Setter
    @Getter
    private String name;
    
    public ColumnImpl(Table table, String name) {
        this.table = table;
        this.name = name;
    }
}
