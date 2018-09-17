package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

import java.util.Objects;
import java.util.Set;

public interface Schema extends PropertiesOwner {
    
    String getName();
    
    Set<Table> getTables();

    default Table getTable(String name) {
        Objects.requireNonNull(name, "name must not be null!");
        for (Table table : this.getTables()) {
            if (name.equalsIgnoreCase(table.getName())) {
                return table;
            }
        }
        return null;
    }

    Set<View> getViews();

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