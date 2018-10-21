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

    default List<Column> getPrimaryColumns() {
        return getColumns().stream().filter(Column::isPrimary).collect(Collectors.toList());
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

    default List<String> getRecord(List<List<String>> values, String... primaryKeys) {
        List<List<String>> possibleResults = new ArrayList<>(values);
        int primaryColumnIndex = 0;
        for (Column primaryColumn : getPrimaryColumns()) {
            String givenPrimaryKey = primaryKeys[primaryColumnIndex];
            int index = indexOf(primaryColumn);
            possibleResults = possibleResults.stream().filter(possibleResult -> givenPrimaryKey.equals(possibleResult.get(index))).collect(Collectors.toList());
            ++primaryColumnIndex;
        }
        return possibleResults.isEmpty() ? null : possibleResults.get(0);
    }

    Storage getStorage();
}
