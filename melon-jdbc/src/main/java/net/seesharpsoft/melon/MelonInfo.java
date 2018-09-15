package net.seesharpsoft.melon;

import lombok.Getter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.commons.collection.PropertiesOwner;

public class MelonInfo implements PropertiesOwner {
    
    public static final String CONFIG_FILE = "configurationFile";
    
    @Getter
    private final String id;

    @Getter
    private final String url;
    
    @Getter
    private final Schema schema;

    @Getter
    private Properties properties;
    
    public MelonInfo(String id, String url, Schema schema, Properties properties) {
        this.url = url;
        this.schema = schema;
        this.properties = new Properties(properties);
        this.id = id;
    }
    
}
