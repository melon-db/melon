package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public interface Table extends PropertiesOwner, NamedEntity {

    /**
     * Returns the schema this table belongs to.
     * @return the schema this table belongs to
     */
    Schema getSchema();

    List<Column> getColumns();

    default Column getColumn(String name) {
        Objects.requireNonNull(name, "name must not be null!");
        for (Column column : getColumns()) {
            if (name.equalsIgnoreCase(column.getName())) {
                return column;
            }
        }
        return null;
    }

    default Column getPrimaryColumn() {
        List<Column> primaryKeyColumns = getColumns().stream().filter(column -> this.equals(column.getTable()) && column.isPrimary()).collect(Collectors.toList());
        if(primaryKeyColumns.size() > 1) {
            throw new UnsupportedOperationException(String.format("not more than one primary key columns allowed! (Table: %s)", this.getName()));
        }
        return primaryKeyColumns.isEmpty() ? null : primaryKeyColumns.get(0);
    }

    default int getPrimaryColumnIndex() {
        Column primaryColumn = getPrimaryColumn();
        if (primaryColumn != null) {
            return indexOf(primaryColumn);
        }
        return -1;
    }

    default int indexOf(Column column) {
        return getColumns().indexOf(column);
    }

    default int indexOf(String columnName) {
        return indexOf(getColumn(columnName));
    }

    default String getValue(List<String> values, Column column) {
        return values.get(this.indexOf(column));
    }

    default List<String> getRecord(List<List<String>> records, String primaryKeyValue) {
        if (primaryKeyValue == null) {
            return null;
        }

        List<List<String>> possibleResults = new ArrayList<>(records);
        int primaryKeyColumnIndex = getPrimaryColumnIndex();
        if(primaryKeyColumnIndex == -1) {
            throw new UnsupportedOperationException(String.format("primary key column required! (Table: %s)", this.getName()));
        }
        possibleResults = possibleResults.stream().filter(possibleResult -> primaryKeyValue.equals(possibleResult.get(primaryKeyColumnIndex))).collect(Collectors.toList());
        return possibleResults.isEmpty() ? null : possibleResults.get(0);
    }

    default List<String> getPrimaryValues(List<List<String>> records) {
        List<String> result = new ArrayList<>();
        int primaryKeyColumnIndex = indexOf(getPrimaryColumn());
        if (primaryKeyColumnIndex != -1) {
            for (List<String> values : records) {
                result.add(values.get(primaryKeyColumnIndex));
            }
        }
        return result;
    }

    default List<Column> getReferenceColumns(Table targetTable) {
        List<Column> referenceColumns = new ArrayList<>();
        for (Column column : getColumns()) {
            if (column.getReference() != null) {
                if (targetTable == null || targetTable.equals(column.getReference())) {
                    referenceColumns.add(column);
                }
            }
        }
        return referenceColumns;
    }

    default List<String> createRecord() {
        List columns = getColumns();
        final List<String> record = new ArrayList<>(columns.size());
        columns.forEach(column -> record.add(null));
        return record;
    }

    Storage getStorage();
}
