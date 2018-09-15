package net.seesharpsoft.melon;

import lombok.Getter;

import java.util.Properties;
import java.util.UUID;

public class MelonInfo {
    
    public static final String CONFIG_FILE = "configurationFile";
    
    @Getter
    private final UUID uuid;

    @Getter
    private final String url;
    
    @Getter
    private final Schema schema;

    @Getter
    private Properties properties;
    
    public MelonInfo(String url, Schema schema, Properties properties) {
        this.url = url;
        this.schema = schema;
        this.properties = properties;
        this.uuid = UUID.randomUUID();
    }
    
}
