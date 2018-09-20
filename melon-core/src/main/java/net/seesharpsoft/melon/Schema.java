package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

import java.util.List;
import java.util.Objects;

public interface Schema extends PropertiesOwner, NamedEntity {
    
    List<Table> getTables();

    default Table getTable(String name) {
        Objects.requireNonNull(name, "name must not be null!");
        for (Table table : this.getTables()) {
            if (name.equalsIgnoreCase(table.getName())) {
                return table;
            }
        }
        return null;
    }

    List<View> getViews();

    default View getView(String name) {
        Objects.requireNonNull(name, "name must not be null!");
        for (View view : this.getViews()) {
            if (name.equalsIgnoreCase(view.getName())) {
                return view;
            }
        }
        return null;
    }
}
