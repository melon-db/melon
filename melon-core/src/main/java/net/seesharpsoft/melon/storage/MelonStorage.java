package net.seesharpsoft.melon.storage;

import lombok.Getter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;

import java.io.IOException;
import java.util.*;

public class MelonStorage extends StorageBase {

    private static class TableValue {
        @Getter
        private final Table table;

        @Getter
        private final List<String> record;

        @Getter
        private final int columnIndex;

        public TableValue(Table table, List<String> record, int columnIndex) {
            this.table = table;
            this.record = record;
            this.columnIndex = columnIndex;
        }

        public String getValue() {
            return getRecord().get(getColumnIndex());
        }

        public Column getColumn() {
            return getTable().getColumns().get(getColumnIndex());
        }
    }

    private Table baseTable;

    public MelonStorage(Table table, Properties properties, Table baseTable) {
        super(table, properties);
        this.baseTable = baseTable;
    }

    protected List<List<String>> getValues(Table table, Map<Table, List<List<String>>> tableMap) throws IOException {
        List<List<String>> values = tableMap.get(table);

        if (values == null) {
            values = table.getStorage().read();
            tableMap.put(table, values);
        }

        return values;
    }

    protected TableValue getTableValue(Map<Table, List<List<String>>> tableMap, List<String> record, String tableColumnName) throws IOException {
        Table currentTable = this.baseTable;
        List<String> currentRecord = record;
        String currentColumnName = tableColumnName;
        int currentIndex = -1;

        int separatorIndex = tableColumnName.indexOf('@');
        if (separatorIndex != -1) {
            currentColumnName = tableColumnName.substring(0, separatorIndex);
            Table referencingTable = this.baseTable.getSchema().getTable(tableColumnName.substring(separatorIndex + 1));
            List<Column> referenceColumns = referencingTable.getReferenceColumns(this.baseTable);
            if (referenceColumns.size() != 1) {
                throw new UnsupportedOperationException(String.format("exactly one reference column expected in table '%s' to table '%s', but found %s", referencingTable, this.baseTable, referenceColumns.size()));
            }
            Column referencingColumn = referenceColumns.get(0);
            String primaryKey = record.get(this.baseTable.getPrimaryColumnIndex());
            List<List<String>> referencingTableRecords = getValues(referencingTable, tableMap);
            List<String> referencingRecord = referencingTableRecords
                    .stream()
                    .filter(targetTableRecord -> primaryKey.equals(referencingTable.getValue(targetTableRecord, referencingColumn)))
                    .findFirst().orElse(null);

            if (referencingRecord == null) {
                referencingRecord = referencingTable.createRecord();
                referencingRecord.set(referencingTable.indexOf(referencingColumn), primaryKey);
                referencingTableRecords.add(referencingRecord);
            }

            currentTable = referencingTable;
            currentRecord = referencingRecord;
        }

        String[] names = currentColumnName.split("\\.");

        for (int currentNameIndex = 0; currentNameIndex < names.length; ++currentNameIndex) {
            currentColumnName = names[currentNameIndex];
            for (Column column : currentTable.getColumns()) {
                if (currentColumnName.equalsIgnoreCase(column.getName())) {
                    currentIndex = currentTable.indexOf(column);
                    if (currentNameIndex < names.length - 1) {
                        currentTable = column.getReference();
                        Objects.requireNonNull(currentTable, String.format("reference not found for '%s'", currentColumnName));
                        if (currentRecord != null) {
                            currentRecord = currentTable.getRecord(getValues(currentTable, tableMap), currentRecord.get(currentIndex));
                        }
                        break;
                    }
                }
            }
        }

        return new TableValue(currentTable, currentRecord, currentIndex);
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
                TableValue tableValue = getTableValue(tableMap, sourceRecord, column.getSource() == null ? column.getName() : column.getSource());
                sourceRecord = tableValue.getRecord();
                if (sourceRecord == null) {
                    currentRecord.add(null);
                } else {
                    currentRecord.add(sourceRecord.get(tableValue.getColumnIndex()));
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
            final List<String> actualRecord = mergeBaseTableRecord(table, baseRecords, record, targetRecords);
            mergeReferenceRecord(table, actualRecord, record, tableMap);
        }

        for (Map.Entry<Table, List<List<String>>> entry : tableMap.entrySet()) {
            entry.getKey().getStorage().write(entry.getValue());
        }

        baseTable.getStorage().write(targetRecords);
    }

    private void mergeReferenceRecord(Table table, List<String> baseRecord, List<String> record, Map<Table, List<List<String>>> tableMap) throws IOException {
        int columnIndex = 0;
        for (Column column : table.getColumns()) {
            int separatorIndex = column.getSource() == null ? -1 : column.getSource().indexOf('@');
            if (separatorIndex != -1) {
                TableValue tableValue = getTableValue(tableMap, record, column.getSource());
                List<String> sourceRecord = tableValue.getRecord();
                if (sourceRecord == null) {
                    continue;
                }
                sourceRecord.set(tableValue.getColumnIndex(), record.get(columnIndex));
            }
            ++columnIndex;
        }
    }

    private List<String> mergeBaseTableRecord(Table table, List<List<String>> baseRecords, List<String> record, List<List<String>> targetRecords) {
        String primaryKeyValue = null;
        Map<Integer, Integer> indexMap = new HashMap<>();
        int columnIndex = 0;
        for (Column column : table.getColumns()) {
            int index;
            if (column.getSource() == null || (column.getSource().indexOf('@') == -1 && column.getSource().indexOf('.') == -1)) {
                index = baseTable.indexOf(column.getSource() == null ? column.getName() : column.getSource());
                if (baseTable.getColumns().get(index).isPrimary()) {
                    primaryKeyValue = record.get(columnIndex);
                }
                indexMap.put(columnIndex, index);
            }
            ++columnIndex;
        }

        List<String> baseRecord = baseTable.getRecord(baseRecords, primaryKeyValue);
        final List<String> actualRecord = baseRecord == null ? baseTable.createRecord() : baseRecord;
        indexMap.forEach((sourceIndex, baseIndex) -> actualRecord.set(baseIndex, record.get(sourceIndex)));
        if (!targetRecords.contains(actualRecord)) {
            targetRecords.add(actualRecord);
        }
        return actualRecord;
    }
}
