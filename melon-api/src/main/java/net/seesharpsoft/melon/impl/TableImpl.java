package net.seesharpsoft.melon.impl;

import lombok.Getter;
import lombok.Setter;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableImpl implements Table {
    
    protected final List<Column> columns;
    
    @Getter
    protected final String name;
    
    @Setter
    @Getter
    protected Storage storage;
    
    public TableImpl(String name) {
        this.name = name;
        this.columns = new ArrayList<>();
    }
    
    @Override
    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }
    
    public void addColumn(Column column) {
        this.columns.add(column);
    }
}
