package net.seesharpsoft.melon.config;

import lombok.Getter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.commons.collection.PropertiesOwner;

public class ConfigBase implements PropertiesOwner {
    
    @Getter
    public Properties properties;
    
    public Properties getProperties(Properties parent) {
        Properties finalProperties = new Properties(parent);
        finalProperties.putAll(properties);
        return new Properties(finalProperties);
    }
}
