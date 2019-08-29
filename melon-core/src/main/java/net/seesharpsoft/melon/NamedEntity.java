package net.seesharpsoft.melon;

import net.seesharpsoft.UnhandledSwitchCaseException;
import net.seesharpsoft.commons.collection.PropertiesOwner;
import net.seesharpsoft.melon.sql.SqlHelper;

public interface NamedEntity extends PropertiesOwner {

    String getName();

    enum Format {
        DEFAULT,
        SANITIZED
    }

    String PROPERTY_TABLE_FORMAT = "-name-format";

    default Format getNameFormat() {
        String tableFormat = getProperties().containsKey(PROPERTY_TABLE_FORMAT) ?
                getProperties().get(PROPERTY_TABLE_FORMAT) :
                Format.DEFAULT.toString();

        return Format.valueOf(tableFormat.toUpperCase());
    }

    default String getFormattedName() {
        Format nameFormat = getNameFormat();
        switch (nameFormat) {
            case DEFAULT:
                return String.format("\"%s\"", getName());
            case SANITIZED:
                return SqlHelper.sanitizeDbName(getName());
            default:
                throw new UnhandledSwitchCaseException(nameFormat);
        }
    }
}
