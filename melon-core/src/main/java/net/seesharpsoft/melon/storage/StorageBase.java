package net.seesharpsoft.melon.storage;

import lombok.Getter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.Table;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static net.seesharpsoft.melon.MelonHelper.deepCopyRecords;

public abstract class StorageBase implements Storage {

    @Getter
    protected final Properties properties;

    protected final Table table;

    protected List<List<String>> currentRecords;

    private boolean dirty = false;

    public StorageBase(Table table, Properties properties) {
        Objects.requireNonNull(table, "table must not be null!");
        Objects.requireNonNull(properties, "properties must not be null!");
        this.table = table;
        this.properties = properties;
        this.isDirty(true);
    }

    protected List<List<String>> validateData(List<List<String>> data, Table table, Properties properties) {
        final List<Column> columns = table.getColumns();
        return data.stream().filter(record -> {
            for (int i = 0; i < columns.size(); ++i) {
                if (columns.get(i).isPrimary() && record.get(i) == null) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
    }

    @Override
    public final List<List<String>> read() throws IOException {
        if (isDirty() || currentRecords == null) {
            List<List<String>> result = read(this.table, this.properties);
            currentRecords = validateData(result, this.table, this.properties);
            isDirty(false);
        }
        return deepCopyRecords(currentRecords);
    }

    @Override
    public final void write(List<List<String>> records) throws IOException {
        if (getProperties().getOrDefault(PROPERTY_STORAGE_MODE, STORAGE_MODE_DEFAULT).equals(STORAGE_MODE_READONLY)) {
            return;
        }
        List<List<String>> actualRecords = read();
        if (!actualRecords.equals(records)) {
            write(this.table, this.properties, records);
            isDirty(true);
        }
    }

    @Override
    public Column getColumn(String name) {
        return this.table.getColumn(name);
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public void isDirty(boolean isDirty) {
        this.dirty = isDirty;
    }

    protected abstract List<List<String>> read(Table table, Properties properties) throws IOException;

    protected abstract void write(Table table, Properties properties, List<List<String>> records) throws IOException;
}
