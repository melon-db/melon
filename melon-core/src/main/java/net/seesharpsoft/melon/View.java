package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.PropertiesOwner;

public interface View extends PropertiesOwner, NamedEntity {
    
    String getQuery();
}
