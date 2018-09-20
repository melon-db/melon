package net.seesharpsoft.melon.impl;

import lombok.Getter;
import lombok.Setter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Schema;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableImpl implements Table {
    
    @Getter
    protected final Properties properties;
    
    protected final List<Column> columns;
    
    @Getter
    protected final String name;

    @Getter
    protected final Schema schema;
    
    @Setter
    @Getter
    protected Storage storage;
    
    public TableImpl(Schema schema, String name, Properties properties) {
        this.schema = schema;
        this.name = name;
        this.properties = new Properties(properties);
        this.columns = new ArrayList<>();
    }
    
    @Override
    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }
    
    public void addColumn(Column column) {
        this.columns.add(column);
    }
    
    @Override
    public String toString() {
        return name;
    }
}
