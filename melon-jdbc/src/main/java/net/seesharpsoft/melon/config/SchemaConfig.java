package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.impl.SchemaImpl;

import java.util.Set;

public class SchemaConfig {
    
    public String name;
    
    public Set<TableConfig> tables;
    
    public Properties properties;
    
    public SchemaImpl getSchema(Properties additionalProperties) {
        Properties finalProperties = new Properties(additionalProperties);
        finalProperties.putAll(properties);
        
        SchemaImpl schema = new SchemaImpl(name, finalProperties);
        tables.forEach(table -> schema.addTable(table.getTable(finalProperties)));
        return schema;
    }
}
