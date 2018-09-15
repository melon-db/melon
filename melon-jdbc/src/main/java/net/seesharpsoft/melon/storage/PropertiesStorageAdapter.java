package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.StorageAdapter;
import net.seesharpsoft.melon.Table;

import java.io.File;
import java.io.IOException;

public class PropertiesStorageAdapter implements StorageAdapter {
    @Override
    public boolean canHandle(Table table, Properties properties, Object input) {
        return input instanceof File &&
                ((File) input).getName().endsWith(".properties");
    }

    @Override
    public Storage createStorage(Table table, Properties properties, Object input) throws IOException {
        return new PropertiesStorage(table, properties, (File)input);
    }
    
    @Override
    public int getPriority() {
        return PRIORITY_LOWEST;
    }
}
