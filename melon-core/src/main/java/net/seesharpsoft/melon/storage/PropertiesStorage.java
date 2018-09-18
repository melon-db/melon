package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PropertiesStorage extends FileStorageBase {

    public PropertiesStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        Properties dataProperties = Properties.read(file);

        return dataProperties.entrySet().stream()
                .map(entry -> {
                    List<String> values = new ArrayList();
                    values.add(entry.getKey().toString());
                    values.add(entry.getValue() == null ? "" : entry.getValue().toString());
                    return values;
                })
                .collect(Collectors.toList());
    }

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        Properties dataProperties = new Properties();

        for (List<String> values : records) {
            dataProperties.put(values.get(0), values.size() > 1 ? values.get(1) : null);
        }
        dataProperties.store(file, false);
    }
}
