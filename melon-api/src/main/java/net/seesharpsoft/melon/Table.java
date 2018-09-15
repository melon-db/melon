package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

import java.util.List;
import java.util.Objects;

public interface Table extends PropertiesOwner {

    String getName();

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
