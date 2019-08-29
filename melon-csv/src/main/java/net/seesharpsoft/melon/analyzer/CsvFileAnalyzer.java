package net.seesharpsoft.melon.analyzer;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.MelonBuilder;
import net.seesharpsoft.melon.config.TableConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;

import static net.seesharpsoft.melon.storage.CsvStorage.*;

public class CsvFileAnalyzer extends FileAnalyzerBase {
    @Override
    public TableConfig analyze(File file, Properties properties) {
        try {
            return getTableConfig(file, properties);
        } catch (IOException exc) {
            exc.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean canHandle(File file) {
        return file.getName().toLowerCase().endsWith(".csv");
    }

    protected boolean hasHeader(Properties properties) {
        return properties.getOrDefault(PROPERTY_HAS_HEADER, DEFAULT_HAS_HEADER);
    }

    protected boolean trimValues(Properties properties) {
        return properties.getOrDefault(PROPERTY_TRIM_VALUES, DEFAULT_TRIM_VALUES);
    }

    protected char delimiter(Properties properties) {
        return properties.getOrDefault(PROPERTY_DELIMITER, DEFAULT_DELIMITER);
    }

    protected CSVFormat getCSVFormat(Properties properties) {
        CSVFormat format = CSVFormat.RFC4180.withTrim(trimValues(properties))
                .withDelimiter(delimiter(properties))
                .withSkipHeaderRecord(false);
        return format;
    }

    protected TableConfig getTableConfig(File file, Properties properties) throws IOException {
        MelonBuilder.SchemaBuilder.TableBuilder tableBuilder = new MelonBuilder()
                .addSchema("default")
                .addTable(file.getName())
                .property(PROPERTY_HAS_HEADER, hasHeader(properties))
                .property(PROPERTY_TRIM_VALUES, trimValues(properties))
                .property(PROPERTY_DELIMITER, delimiter(properties));

        Properties currentProperties = tableBuilder.getTableConfig().getProperties();
        try (
                Reader reader = getReader(file, currentProperties);
                CSVParser csvParser = new CSVParser(reader, getCSVFormat(currentProperties))
        ) {
            for (CSVRecord record : csvParser) {
                Iterator<String> valueIterator = record.iterator();
                while (valueIterator.hasNext()) {
                    tableBuilder.addColumn(valueIterator.next());
                }
                break;
            }
        }
        return tableBuilder.getTableConfig();
    }
}
