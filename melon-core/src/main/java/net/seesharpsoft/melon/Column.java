package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

public interface Column extends PropertiesOwner {
    Table getTable();

    String getName();
    
    boolean isPrimary();
}
