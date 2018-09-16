package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.config.SchemaConfig;
import net.seesharpsoft.melon.jdbc.MelonDriver;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class MelonadeFactory {
    
    public static MelonadeFactory INSTANCE = new MelonadeFactory();

    private final Map<String, Melonade> CREATED_INFOS = new HashMap<>();

    public void clear() {
        CREATED_INFOS.clear();
    }

    public void remove(Melonade melonade) {
        CREATED_INFOS.remove(melonade);
    }

    public static File getAbsolutePath(String fileName, String reference) {
        String path = fileName;
        if (reference != null && !fileName.startsWith("/") && !fileName.startsWith("\\")) {
            path = reference + File.separator + fileName;
        }
        URL url = Melonade.class.getResource(path);
        if (url == null) {
            return new File(path);
        }
        return new File(url.getFile());
    }

    private static SchemaConfig getSchemaConfigFromStream(InputStream stream) {
        Yaml yaml = new Yaml();
        return yaml.loadAs(stream, SchemaConfig.class);
    }

    public Melonade getOrCreateMelonade(String url, Properties properties) throws IOException {
        Melonade melonade = CREATED_INFOS.get(url);

        if (melonade == null) {
            String configFile = url.replaceFirst(Pattern.quote(MelonDriver.MELON_URL_PREFIX), "");
            File file = getAbsolutePath(configFile, null);
            SchemaConfig schemaConfig = null;
            try (InputStream resourceStream = SharpIO.createInputStream(configFile, true)) {
                if (resourceStream == null) {
                    try (InputStream fileStream = SharpIO.createInputStream(configFile, false)) {
                        if (fileStream == null) {
                            throw new IOException(configFile + " not found");
                        }
                        schemaConfig = getSchemaConfigFromStream(fileStream);
                    }
                } else {
                    schemaConfig = getSchemaConfigFromStream(resourceStream);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            Properties infoProperties = new Properties(properties);
            infoProperties.put(Melonade.CONFIG_FILE, file);

            melonade = new Melonade(file.getName(), url, schemaConfig.getSchema(infoProperties), infoProperties);
            CREATED_INFOS.put(url, melonade);
        }

        return melonade;
    }
    
}
