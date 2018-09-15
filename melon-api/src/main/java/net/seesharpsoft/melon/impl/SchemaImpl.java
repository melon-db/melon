package net.seesharpsoft.melon.impl;

import lombok.Getter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Schema;
import net.seesharpsoft.melon.Table;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SchemaImpl implements Schema {
    
    @Getter
    protected final Properties properties;
    
    protected final Set<Table> tables;
    
    @Getter
    private final String name;
    
    public SchemaImpl(String name, Properties properties) {
        this.name = name;
        this.properties = new Properties(properties);
        this.tables = new HashSet<>();
    }

    @Override
    public Set<Table> getTables() {
        return Collections.unmodifiableSet(tables);
    }

    public void addTable(Table table) {
        this.tables.add(table);
    }
}
