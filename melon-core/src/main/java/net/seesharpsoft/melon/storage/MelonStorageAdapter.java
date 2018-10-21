package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.StorageAdapter;
import net.seesharpsoft.melon.Table;

import java.io.IOException;

public class MelonStorageAdapter implements StorageAdapter {
    @Override
    public boolean canHandle(Table table, Properties properties, Object input) {
        return input instanceof Table;
    }

    @Override
    public Storage createStorage(Table table, Properties properties, Object input) throws IOException {
        return new MelonStorage(table, properties, (Table)input);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }
}
