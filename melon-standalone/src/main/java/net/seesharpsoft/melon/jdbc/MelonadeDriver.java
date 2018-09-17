package net.seesharpsoft.melon.jdbc;


import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class MelonadeDriver extends MelonDriver {

    public static final String MELON_STANDALONE_URL_PREFIX = "jdbc:melonade:";
    
    /**
     * The database URL prefix of this database.
     */
    private static final MelonadeDriver INSTANCE = new MelonadeDriver();

    private static volatile boolean registered;
    
    static {
        MelonadeDriver.load();
    }
    
    @Override
    public boolean acceptsURL(String url) {
        if (url != null && url.startsWith(MELON_STANDALONE_URL_PREFIX)) {
            return true;
        }
        return false;
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
            return new MelonadeConnection(url, properties);
        } catch (Exception e) {
            e.printStackTrace();
            throw new SQLException(e);
        }
    }

    /**
     * INTERNAL
     */
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
