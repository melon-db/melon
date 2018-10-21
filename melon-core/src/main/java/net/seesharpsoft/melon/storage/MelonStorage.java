package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MelonStorage extends StorageBase {

    private Table baseTable;

    public MelonStorage(Table table, Properties properties, Table baseTable) {
        super(table, properties);
        this.baseTable = baseTable;
    }

    @Override
    public Column getColumn(String name) {
        return this.baseTable.getColumn(name);
    }

    protected List<List<String>> getValues(Table table, Map<Table, List<List<String>>> tableMap) throws IOException {
        List<List<String>> values = tableMap.get(table);

        if (values == null) {
            values = table.getStorage().read();
            tableMap.put(table, values);
        }

        return values;
    }

    @Override
    protected List<List<String>> read(Table table, Properties properties) throws IOException {
        Map<Table, List<List<String>>> tableMap = new HashMap<>();
        List<List<String>> sourceRecords = getValues(baseTable, tableMap);
        List<List<String>> targetRecords = new ArrayList<>();

        for (List<String> record : sourceRecords) {
            List<String> currentRecord = new ArrayList<>();
            for (Column column : table.getColumns()) {
                List<String> sourceRecord = record;
                int index = -1;
                if (column.getSource() == null || column.getSource().indexOf('.') == -1) {
                    index = baseTable.indexOf(column.getSource() == null ? column.getName() : column.getSource());
                } else {
                    String baseTableColumnName = column.getSource().substring(0, column.getSource().indexOf('.'));
                    Column baseTableColumn = this.baseTable.getColumn(baseTableColumnName);
                    Table sourceTable = baseTableColumn.getReference();
                    List<List<String>> currentValues = getValues(sourceTable, tableMap);
                    sourceRecord = sourceTable.getRecord(currentValues, baseTable.getValue(sourceRecord, baseTableColumn));
                    index = sourceTable.indexOf(column.getSource());
                }
                if (sourceRecord == null) {
                    currentRecord.add(null);
                } else {
                    currentRecord.add(sourceRecord.get(index));
                }
            }
            targetRecords.add(currentRecord);
        }

        return targetRecords;
    }

    @Override
    protected void write(Table table, Properties properties, List<List<String>> records) throws IOException {
        Map<Table, List<List<String>>> tableMap = new HashMap<>();
        List<List<String>> baseRecords = baseTable.getStorage().read();
        List<List<String>> targetRecords = new ArrayList<>();

        for (List<String> record : records) {
            final List<String> baseRecord = mergeBaseTableRecord(table, baseRecords, record, targetRecords);
            mergeReferenceRecord(table, baseRecord, record, tableMap);
        }

        for (Map.Entry<Table, List<List<String>>> entry : tableMap.entrySet()) {
            entry.getKey().getStorage().write(entry.getValue());
        }

        baseTable.getStorage().write(targetRecords);
    }

    private void mergeReferenceRecord(Table table, List<String> baseRecord, List<String> record, Map<Table, List<List<String>>> tableMap) throws IOException {
        int columnIndex = 0;
        for (Column column : table.getColumns()) {
            int index = -1;
            if (column.getSource() != null && column.getSource().indexOf('.') != -1) {
                String baseTableColumnName = column.getSource().substring(0, column.getSource().indexOf('.'));
                Column baseTableColumn = this.baseTable.getColumn(baseTableColumnName);
                Table sourceTable = baseTableColumn.getReference();
                List<List<String>> currentValues = getValues(sourceTable, tableMap);
                List<String> sourceRecord = sourceTable.getRecord(currentValues, baseTable.getValue(baseRecord, baseTableColumn));

                if (sourceRecord == null) {
                    continue;
                }

                index = sourceTable.indexOf(column.getSource());
                sourceRecord.set(index, record.get(columnIndex));
            }
            ++columnIndex;
        }
    }

    private List<String> mergeBaseTableRecord(Table table, List<List<String>> baseRecords, List<String> record, List<List<String>> targetRecords) {
        List<String> primaryValues = new ArrayList<>();
        Map<Integer, Integer> indexMap = new HashMap<>();
        int columnIndex = 0;
        for (Column column : table.getColumns()) {
            int index = -1;
            if (column.getSource() == null || column.getSource().indexOf('.') == -1) {
                index = baseTable.indexOf(column.getSource() == null ? column.getName() : column.getSource());
                if (baseTable.getColumns().get(index).isPrimary()) {
                    primaryValues.add(record.get(columnIndex));
                }
                indexMap.put(columnIndex, index);
            }
            ++columnIndex;
        }

        final List<String> baseRecord = baseTable.getRecord(baseRecords, primaryValues.toArray(new String[0]));
        indexMap.forEach((sourceIndex, baseIndex) -> baseRecord.set(baseIndex, record.get(sourceIndex)));
        if (!targetRecords.contains(baseRecord)) {
            targetRecords.add(baseRecord);
        }
        return baseRecord;
    }
}
