package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.impl.SchemaImpl;

import java.util.Set;

public class SchemaConfig extends ConfigBase {
    
    public String name;
    
    public Set<TableConfig> tables;

    public Set<ViewConfig> views;
    
    public SchemaImpl getSchema(Properties additionalProperties) {
        Properties finalProperties = getProperties(additionalProperties);
        
        SchemaImpl schema = new SchemaImpl(name, finalProperties);
        if (tables != null) {
            tables.forEach(table -> schema.addTable(table.getTable(finalProperties)));
        }
        if (views != null) {
            views.forEach(view -> schema.addView(view.getView(finalProperties)));
        }
        return schema;
    }
}
