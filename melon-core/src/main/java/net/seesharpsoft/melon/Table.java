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

        String[] names = name.split("\\.");

        Table table = this;

        for (int currentNameIndex = 0; currentNameIndex < names.length; ++currentNameIndex) {
            String currentName = names[currentNameIndex];
            for (Column column : table.getColumns()) {
                if (currentName.equalsIgnoreCase(column.getName())) {
                    if (currentNameIndex < names.length - 1) {
                        table = column.getReference();
                        Objects.requireNonNull(table, String.format("reference not found for '%s'", name));
                        break;
                    }
                    return column;
                }
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

    Storage getStorage();
}
