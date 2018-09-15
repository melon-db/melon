package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.melon.Melonade;
import org.h2.message.DbException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            if (info == null) {
                info = new Properties();
            }
            if (!acceptsURL(url)) {
                return null;
            }
            return new MelonConnection(Melonade.getOrCreateMelonInfo(url, info));
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }
}
