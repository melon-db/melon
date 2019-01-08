package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.config.SchemaConfig;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class MelonFactory {

    public static MelonFactory INSTANCE = new MelonFactory();

    private final Map<String, Melon> CREATED_INFOS = new HashMap<>();

    public void clear() {
        CREATED_INFOS.clear();
    }

    public void remove(Melon melon) {
        melon.setReferenceCounter(melon.getReferenceCounter() - 1);
        if (melon.getReferenceCounter() == 0) {
            CREATED_INFOS.remove(melon.getUrl());
        }
    }

    private static SchemaConfig getSchemaConfigFromStream(InputStream stream) {
        Yaml yaml = new Yaml();
        return yaml.loadAs(stream, SchemaConfig.class);
    }

    public static final String getConfigFilePath(java.util.Properties properties) {
        Object configFile = properties.get(Constants.PROPERTY_CONFIG_FILE);
        return configFile == null ? null : configFile.toString();
    }

    public Melon getOrCreateMelon(String url, java.util.Properties properties) throws IOException {
        Melon melon = CREATED_INFOS.get(url);

        if (melon == null) {
            String configFile = getConfigFilePath(properties);
            File file = SharpIO.getFile(configFile);
            SchemaConfig schemaConfig = null;
            try (InputStream resourceStream = SharpIO.createInputStream(configFile)) {
                schemaConfig = getSchemaConfigFromStream(resourceStream);
            } catch (IOException e) {
                e.printStackTrace();
            }

            Properties infoProperties = new Properties();
            infoProperties.putAll(properties);
            infoProperties.put(Constants.PROPERTY_CONFIG_FILE, file);

            melon = new Melon(url, schemaConfig.getSchema(infoProperties), infoProperties);
            CREATED_INFOS.put(url, melon);
        }

        melon.setReferenceCounter(melon.getReferenceCounter() + 1);
        return melon;
    }

}
