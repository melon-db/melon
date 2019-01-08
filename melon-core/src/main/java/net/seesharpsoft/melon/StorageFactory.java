package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.commons.util.SharpIO;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ServiceLoader;
import java.util.Set;


public class StorageFactory {

    private static ServiceLoader<StorageAdapter> adapterServiceLoader = ServiceLoader.load(StorageAdapter.class);

    public static final String MELON_STORAGE_PROTOCOL = "melon";

    /**
     * Singleton.
     */
    public static final StorageFactory INSTANCE = new StorageFactory();

    public Storage createStorageFor(Table table, String uri, Properties properties) throws IOException {
        Object input = getStorageInput(table, uri, properties);
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

    protected Object getStorageInput(Table table, String uri, Properties properties) {
        if (uri == null || uri.trim().isEmpty()) {
            return null;
        }
        if (uri.startsWith(String.format("%s://",MELON_STORAGE_PROTOCOL))) {
            int lastSlashIndex = uri.lastIndexOf('/');
            String tableName = uri.substring(lastSlashIndex + 1);
            return table.getSchema().getTable(tableName);
        }
        return SharpIO.getFile(uri, ((File) properties.get(Constants.PROPERTY_CONFIG_FILE)).getParentFile().getAbsolutePath());
    }
}
