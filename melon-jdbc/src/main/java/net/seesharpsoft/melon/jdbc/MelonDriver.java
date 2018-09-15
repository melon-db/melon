package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Melonade;
import org.h2.message.DbException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MelonDriver extends org.h2.Driver {

    /**
     * The database URL prefix of this database.
     */
    public static final String MELON_URL_PREFIX = "jdbc:melon:";
    
    private static final MelonDriver INSTANCE = new MelonDriver();
    
    static {
        try {
            DriverManager.registerDriver(INSTANCE);
        } catch (SQLException e) {
            DbException.traceThrowable(e);
        }
    }
    
    @Override
    public boolean acceptsURL(String url) {
        if (url != null && url.startsWith(MELON_URL_PREFIX)) {
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
            return new MelonConnection(Melonade.getOrCreateMelonInfo(url, properties));
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }
}
