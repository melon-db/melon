package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class CsvStorage extends FileStorageBase {

    public static final String HAS_HEADER = "csv-header";
    public static final boolean DEFAULT_HAS_HEADER = true;

    public static final String PROPERTY_TRIM_VALUES = "csv-trim";
    public static final boolean DEFAULT_TRIM_VALUES = true;

    public static final String PROPERTY_DELIMITER = "csv-delimiter";
    public static final char DEFAULT_DELIMITER = ',';

    public CsvStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    protected boolean hasHeader() {
        return properties.getOrDefault(HAS_HEADER, DEFAULT_HAS_HEADER);
    }

    protected boolean trimValues() {
        return properties.getOrDefault(PROPERTY_TRIM_VALUES, DEFAULT_TRIM_VALUES);
    }

    protected char delimiter() {
        return properties.getOrDefault(PROPERTY_DELIMITER, DEFAULT_DELIMITER);
    }

    protected CSVFormat getCSVFormat(boolean writing) {
        CSVFormat format = CSVFormat.RFC4180.withTrim(trimValues())
                .withDelimiter(delimiter())
                .withHeader(table.getColumns().stream().map(column -> column.getName()).collect(Collectors.toList()).toArray(new String[0]))
                .withSkipHeaderRecord(writing ? !hasHeader() : hasHeader());
        return format;
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        List<List<String>> records = new ArrayList<>();
        List<Column> columns = table.getColumns();
        try (
                Reader reader = getReader(file);
                CSVParser csvParser = new CSVParser(reader, getCSVFormat(false))
        ) {
            for (CSVRecord record : csvParser) {
                List<String> values = new ArrayList<>();
                for (int i = 0; i < columns.size(); ++i) {
                    String value = i < record.size() ? record.get(i) : null;
                    values.add(value != null && value.isEmpty() ? null : value);
                }
                records.add(values);
            }
        }
        return records;
    }

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        try (
                Writer writer = getWriter(file);
                CSVPrinter csvPrinter = new CSVPrinter(writer, getCSVFormat(true))
        ) {
            for (List<String> values : records) {
                csvPrinter.printRecord(values);
            }
            csvPrinter.flush();
        }
    }
}
