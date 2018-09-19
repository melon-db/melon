package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.melon.MelonFactory;

import java.io.File;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static net.seesharpsoft.melon.Constants.PROPERTY_CONFIG_FILE;
import static net.seesharpsoft.melon.jdbc.MelonDriver.MELON_URL_PREFIX;

public class MelonDbDriver implements Driver {

    public static final String MELON_DB_URL_PREFIX = "jdbc:melondb:";
    public static final String MELON_DB_DRIVER_URL = "h2:mem:";
    public static final String PROPERTY_DB_NAME = "melonName";
    
    private static final MelonDbDriver INSTANCE = new MelonDbDriver();

    private static volatile boolean registered;
    
    static {
        MelonDbDriver.load();
    }

    public static final String getStandaloneConfigFilePath(String connectionUrl, Properties properties) {
        String configFilePath = MelonFactory.getConfigFilePath(properties);
        if (configFilePath == null) {
            configFilePath = connectionUrl.replaceFirst(Pattern.quote(MelonDbDriver.MELON_DB_URL_PREFIX), "");
            if (configFilePath.startsWith(":")) {
                configFilePath = configFilePath.substring(1);
            }
        }
        return configFilePath;
    }

    private static Properties getMelonDbProperties(String configFile, Properties properties) {
        Properties resultProperties = new Properties();
        if (properties != null) {
            resultProperties.putAll(properties);
        }
        resultProperties.put(PROPERTY_CONFIG_FILE, configFile);
        resultProperties.put("AUTOCOMMIT", "false");
        return resultProperties;
    }
    
    @Override
    public boolean acceptsURL(String url) {
        if (url != null && url.startsWith(MELON_DB_URL_PREFIX)) {
            return true;
        }
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 3;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public Connection connect(String url, java.util.Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String configFile = getStandaloneConfigFilePath(url, info);
        Properties melonInfo = getMelonDbProperties(configFile, info);
        String melonUrl = String.format("%s%s%s",
                MELON_URL_PREFIX,
                MELON_DB_DRIVER_URL,
                melonInfo.getOrDefault(PROPERTY_DB_NAME, new File(configFile).getName()).toString());
        
        return DriverManager.getConnection(melonUrl, melonInfo);
    }

    /**
     * INTERNAL
     */
    public static synchronized Driver load() {
        try {
            if (!registered) {
                registered = true;
                DriverManager.registerDriver(INSTANCE);
                org.h2.Driver.load();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return INSTANCE;
    }
}
