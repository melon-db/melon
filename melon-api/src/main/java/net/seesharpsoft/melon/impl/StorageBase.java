package net.seesharpsoft.melon.impl;

import lombok.Getter;
import lombok.Setter;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.Table;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public abstract class StorageBase implements Storage {
    
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
        setLastSynced(Instant.now().toEpochMilli());
        return result;
    }

    @Override
    public void write(List<List<String>> records) throws IOException {
        write(this.table, this.properties, records);
        setLastSynced(Instant.now().toEpochMilli());
    }

    @Override
    public Boolean hasChanges() {
        return getLastSynced() == 0 ? null : getLastModified() > getLastSynced();
    }

    protected long getLastModified() {
        return getLastSynced();
    }
    
    protected abstract List<List<String>> read(Table table, Properties properties) throws IOException;

    protected abstract void write(Table table, Properties properties, List<List<String>> records) throws IOException;
}
