package net.seesharpsoft.melon.config;

import net.seesharpsoft.melon.impl.SchemaImpl;

import java.util.Properties;
import java.util.Set;

public class SchemaConfig {
    
    public String name;
    
    public Set<TableConfig> tables;
    
    public SchemaImpl getSchema(Properties properties) {
        SchemaImpl schema = new SchemaImpl(name);
        tables.forEach(entity -> schema.addTable(entity.getTable(properties)));
        return schema;
    }
}
