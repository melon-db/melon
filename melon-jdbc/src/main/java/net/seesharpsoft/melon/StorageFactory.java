package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;


public class StorageFactory {

    private static ServiceLoader<StorageAdapter> adapterServiceLoader = ServiceLoader.load(StorageAdapter.class);
    
    /**
     * Singleton.
     */
    public static final StorageFactory INSTANCE = new StorageFactory();
    
    /**
     * Private constructor.
     */
    public Storage createStorageFor(Table table, Properties properties, Object input) throws IOException {
        Set<StorageAdapter> foundAdapters = new HashSet<>();
        Properties propertiesCopy = new Properties();
        if (properties != null) {
            propertiesCopy.putAll(properties);
        }
        
        Iterator<StorageAdapter> storageAdapterIterator = adapterServiceLoader.iterator();
        while(storageAdapterIterator.hasNext()) {
            StorageAdapter storageAdapter = storageAdapterIterator.next();
            if (storageAdapter.canHandle(table, propertiesCopy, input)) {
                foundAdapters.add(storageAdapter);
            }
        }
        StorageAdapter storageAdapter = foundAdapters.stream()
                .sorted()
                .findFirst().orElse(null);
        
        return storageAdapter == null ? null : storageAdapter.createStorage(table, propertiesCopy, input);

    }
}
