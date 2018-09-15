package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.*;

import java.io.File;
import java.io.IOException;

public class StorageConfig {
    public String location;
    
    public Properties properties;
    
    public Storage getStorage(Table table, Properties additionalProperties) {
        try {
            Properties finalProperties = new Properties();
            if (additionalProperties != null) {
                finalProperties.putAll(additionalProperties);
            }
            if (this.properties != null) {
                finalProperties.putAll(this.properties);
            }
            if (location != null && !location.isEmpty()) {
                File targetFile = Melonade.getAbsolutePath(location, ((File) additionalProperties.get(MelonInfo.CONFIG_FILE)).getParentFile().getAbsolutePath());

                return StorageFactory.INSTANCE.createStorageFor(table, properties, targetFile);
            }
            return StorageFactory.INSTANCE.createStorageFor(table, properties, null);
        } catch (IOException exc) {
            exc.printStackTrace();
        }
        return null;
    }
}
