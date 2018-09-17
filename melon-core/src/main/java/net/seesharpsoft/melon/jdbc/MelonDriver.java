package net.seesharpsoft.melon.jdbc;


import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class MelonDriver implements Driver {

    /**
     * The database URL prefix of this database.
     */
    public static final String MELON_URL_PREFIX = "jdbc:melon:";
    
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

    @Override
    public Connection connect(String url, java.util.Properties info) throws SQLException {
        try {
            Properties properties = new Properties();
            if (info != null) {
                properties.putAll(info);
            }
            if (!acceptsURL(url)) {
                return null;
            }
            return new MelonConnection(url, properties);
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
