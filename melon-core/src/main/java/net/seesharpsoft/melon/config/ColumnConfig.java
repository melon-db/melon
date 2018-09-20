package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.ColumnImpl;

import java.util.Objects;

public class ColumnConfig extends ConfigBase {
    
    public String name;
    
    public boolean primary = false;
    
    public String reference = null;
    
    public ColumnImpl getColumn(Table table, Properties additionalProperties) {
        ColumnImpl column = new ColumnImpl(table, name, getProperties(additionalProperties));
        column.setPrimary(primary);
        if (reference != null && !reference.isEmpty()) {
            Table referencedTable = table.getSchema().getTable(reference);
            Objects.requireNonNull(referencedTable, String.format("referenced table '%s' not found", referencedTable));
            column.setReference(referencedTable);
        }
        return column;
    }
}
