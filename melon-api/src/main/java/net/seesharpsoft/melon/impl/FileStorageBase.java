package net.seesharpsoft.melon.impl;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public abstract class FileStorageBase extends StorageBase {
    
    protected File file;
    
    public FileStorageBase(Table table, Properties properties, File file) throws IOException {
        super(table, properties);
        Objects.requireNonNull(file, "file must not be null!");
        setFile(file);
    }

    protected void setFile(File file) throws IOException {
        this.file = file;
        if (!file.exists()) {
            file.createNewFile();
        }
    }
    
    protected List<List<String>> read(Table table, Properties properties) throws IOException {
        return read(file, table, properties);
    }
    
    protected void write(Table table, Properties properties, List<List<String>> records) throws IOException {
        write(file, table, properties, records);
    }

    protected abstract List<List<String>> read(File file, Table table, Properties properties) throws IOException;

    protected abstract void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException;
}
