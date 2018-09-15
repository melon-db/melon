package net.seesharpsoft.melon;

import java.util.Objects;
import java.util.Set;

public interface Schema {
    
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
}
