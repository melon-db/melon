package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

public interface Column extends PropertiesOwner {

    String PROPERTY_LENGTH = "column-length";
    int DEFAULT_LENGTH = 255;

    Table getTable();

    String getName();
    
    boolean isPrimary();
}
