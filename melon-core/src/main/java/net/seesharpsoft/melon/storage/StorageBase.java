package net.seesharpsoft.melon.storage;

import lombok.Getter;
import lombok.Setter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.Table;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public abstract class StorageBase implements Storage {
    
    @Getter
    protected final Properties properties;
    
    protected final Table table;

    @Setter
    @Getter
    private long lastSynced;
    
    public StorageBase(Table table, Properties properties) {
        Objects.requireNonNull(table, "table must not be null!");
        Objects.requireNonNull(properties, "properties must not be null!");
        this.table = table;
        this.properties = properties;
        this.lastSynced = 0;
    }
    
    @Override
    public List<List<String>> read() throws IOException {
        List<List<String>> result = read(this.table, this.properties);
        setLastSynced(getSyncTime());
        return result;
    }

    @Override
    public void write(List<List<String>> records) throws IOException {
        if (getProperties().getOrDefault(PROPERTY_ACCESS_MODE, ACCESS_MODE_DEFAULT).equals(ACCESS_MODE_READONLY)) {
            return;
        }
        write(this.table, this.properties, records);
        setLastSynced(getSyncTime());
    }

    @Override
    public boolean hasChanges() {
        return getLastSynced() == 0 || getLastModified() > getLastSynced();
    }

    protected long getLastModified() {
        return getLastSynced();
    }

    protected long getSyncTime() {
        return Instant.now().toEpochMilli();
    }
    
    protected abstract List<List<String>> read(Table table, Properties properties) throws IOException;

    protected abstract void write(Table table, Properties properties, List<List<String>> records) throws IOException;
}
