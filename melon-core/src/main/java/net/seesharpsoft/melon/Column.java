package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

public interface Column extends PropertiesOwner, NamedEntity {

    String PROPERTY_LENGTH = "column-length";
    int DEFAULT_LENGTH = 255;

    Table getTable();

    boolean isPrimary();

    /**
     * Returns the table this column references to.
     * @return the referenced table, null if column is not a reference
     */
    Table getReference();
}
