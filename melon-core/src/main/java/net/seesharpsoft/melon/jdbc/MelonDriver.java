package net.seesharpsoft.melon.jdbc;


import net.seesharpsoft.melon.MelonFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class MelonDriver implements Driver {
    
    /**
     * The database URL prefix of this database.
     */
    public static final String MELON_URL_PREFIX = "jdbc:melon:";
    
    public static final String PROPERTY_DRIVER_JAR = "driverPath";
    public static final String PROPERTY_DRIVER_NAME = "driverName";

    private static final MelonDriver INSTANCE = new MelonDriver();

    private static volatile boolean registered;
    
    static {
        MelonDriver.load();
    }
    
    @Override
    public boolean acceptsURL(String url) {
        if (url != null && url.startsWith(MELON_URL_PREFIX)) {
            return true;
        }
        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 2;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    protected static void requireDependentDriver(Properties properties) throws ClassNotFoundException {
        ClassLoader classLoader = MelonDriver.class.getClassLoader();
        if (properties.containsKey(PROPERTY_DRIVER_JAR)) {
            try {
                classLoader = new URLClassLoader(new URL[]{new URL(properties.getProperty(PROPERTY_DRIVER_JAR))}, MelonDriver.class.getClassLoader());
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
        if (properties.containsKey(PROPERTY_DRIVER_NAME)) {
            Class.forName(properties.getProperty(PROPERTY_DRIVER_NAME), true, classLoader);
        }
    }
    
    @Override
    public Connection connect(String url, java.util.Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        try {
            requireDependentDriver(info);
            return new MelonConnection(
                    DriverManager.getConnection(url.replaceFirst("\\:melon\\:", ":"), info),
                    MelonFactory.INSTANCE.getOrCreateMelon(url, info));
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException(e);
        }
    }

    public static synchronized Driver load() {
        try {
            if (!registered) {
                registered = true;
                DriverManager.registerDriver(INSTANCE);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return INSTANCE;
    }
}
