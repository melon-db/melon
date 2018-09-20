package net.seesharpsoft.melon.impl;

import lombok.Getter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Schema;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.View;

import java.util.*;

public class SchemaImpl implements Schema {
    
    @Getter
    protected final Properties properties;
    
    protected final List<Table> tables;

    protected final List<View> views;
    
    @Getter
    private final String name;
    
    public SchemaImpl(String name, Properties properties) {
        this.name = name;
        this.properties = new Properties(properties);
        this.tables = new ArrayList<>();
        this.views = new ArrayList<>();
    }

    @Override
    public List<Table> getTables() {
        return Collections.unmodifiableList(tables);
    }

    public void addTable(Table table) {
        this.tables.add(table);
    }
    
    @Override
    public List<View> getViews() {
        return Collections.unmodifiableList(views);
    }

    public void addView(View view) {
        this.views.add(view);
    }

}
