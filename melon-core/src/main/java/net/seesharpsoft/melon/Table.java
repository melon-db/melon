package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

import java.util.List;
import java.util.Objects;

public interface Table extends PropertiesOwner, NamedEntity {

    /**
     * Returns the schema this table belongs to.
     * @return the schema this table belongs to
     */
    Schema getSchema();
    
    List<Column> getColumns();

    default Column getColumn(String name) {
        Objects.requireNonNull(name, "name must not be null!");
        for (Column column : this.getColumns()) {
            if (name.equalsIgnoreCase(column.getName())) {
                return column;
            }
        }
        return null;
    }

    Storage getStorage();
}
