package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.StorageFactory;
import net.seesharpsoft.melon.Table;

import java.io.IOException;

public class StorageConfig extends ConfigBase {
    public String uri;

    public Storage getStorage(Table table, Properties additionalProperties) {
        Properties finalProperties = getProperties(additionalProperties);
        try {
            return StorageFactory.INSTANCE.createStorageFor(table, uri, finalProperties);
        } catch (IOException exc) {
            exc.printStackTrace();
        }
        return null;
    }
}
