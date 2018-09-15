package net.seesharpsoft.melon;

import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.config.SchemaConfig;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.jdbc.MelonDriver;
import net.seesharpsoft.melon.sql.SqlHelper;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

public class Melonade {
    
    private static final Map<String, MelonInfo> CREATED_INFOS = new HashMap<>();
    
    public static void reset() {
        CREATED_INFOS.clear();
    }

    public static void close(MelonConnection connection) {
        CREATED_INFOS.remove(connection.getMelonInfo().getUrl());
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
    
    public static MelonInfo getOrCreateMelonInfo(String url, Properties properties) throws IOException {
        MelonInfo melonInfo = CREATED_INFOS.get(url);
        
        if (melonInfo == null) {
            String configFile = url.replaceFirst(Pattern.quote(MelonDriver.MELON_URL_PREFIX), "");
            File file = getAbsolutePath(configFile, null);
            SchemaConfig schemaConfig = null;
            try (InputStream resourceStream = SharpIO.createInputStream(configFile, true)) {
                if (resourceStream == null) {
                    try (InputStream fileStream = SharpIO.createInputStream(configFile, false)) {
                        schemaConfig = getSchemaConfigFromStream(fileStream);
                    }
                } else {
                    schemaConfig = getSchemaConfigFromStream(resourceStream);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            Properties infoProperties = new Properties(properties);
            infoProperties.put(MelonInfo.CONFIG_FILE, file);
            
            melonInfo = new MelonInfo(url, schemaConfig.getSchema(infoProperties), infoProperties);
            CREATED_INFOS.put(url, melonInfo);
        }
        
        return melonInfo;
    }
    
    private static void clearDatabaseTable(Connection connection, Table table) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateClearTableStatement(table))) {
            ps.execute();
        }
    }
    
    public static void syncToDatabase(MelonConnection connection, Table table, List<List<String>> records) throws SQLException {
        clearDatabaseTable(connection, table);
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateInsertStatement(table))) {
            final int batchSize = 1000;
            int count = 0;
            int columnSize = table.getColumns().size();

            for (List<String> fields : records) {
                for (int i = 0; i < columnSize; ++i) {
                    ps.setString(i + 1, fields.get(i));
                }
                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        connection.commit();
    }
    
    public static void syncToDatabase(MelonConnection melonConnection) throws SQLException, IOException {
        MelonInfo info = melonConnection.getMelonInfo();
        
        for (Table table : info.getSchema().getTables()) {
            Storage storage = table.getStorage();
            Boolean storageHasChanges = storage.hasChanges();
            if (storageHasChanges == null) {
                createDatabaseSchema(melonConnection, table);
                storageHasChanges = true;
            }
            if (storageHasChanges) {
                syncToDatabase(melonConnection, table, storage.read());
            }
        }
    }

    private static void createDatabaseSchema(MelonConnection connection, Table table) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateCreateTableStatement(table))) {
            ps.execute();
        }
    }

    public static void syncToStorage(MelonConnection melonConnection) throws SQLException, IOException {
        MelonInfo info = melonConnection.getMelonInfo();

        for (Table table : info.getSchema().getTables()) {
            Storage storage = table.getStorage();
            syncToStorage(melonConnection, table, storage);
        }
    }

    private static void syncToStorage(MelonConnection connection, Table table, Storage storage) throws SQLException, IOException {
        List<List<String>> records = null;
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateSelectStatement(table))) {
            try (ResultSet rs = ps.executeQuery()) {
                records = SqlHelper.fromResultSet(rs);
            }
        }
        storage.write(records);
    }
}
