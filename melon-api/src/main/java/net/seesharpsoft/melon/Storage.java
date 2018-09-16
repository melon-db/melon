package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

import java.io.IOException;
import java.util.List;

public interface Storage extends PropertiesOwner {
    
    String PROPERTY_ACCESS_MODE = "accessMode";

    String ACCESS_MODE_DEFAULT = "Default";

    String ACCESS_MODE_READONLY = "ReadOnly";
    
    List<List<String>> read() throws IOException;

    void write(List<List<String>> records) throws IOException;

    /**
     * Returns whether the storage-data has changed since last call of read or write.
     *
     * @return true if storage data has changed since last call of read or write, false if no changes and NULL if neither records were read or written
     */
    boolean hasChanges() throws IOException;
}
