package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Melonade;
import net.seesharpsoft.melon.MelonadeFactory;
import org.h2.engine.Constants;

import java.sql.SQLException;
import java.sql.Savepoint;

public class MelonConnection extends org.h2.jdbc.JdbcConnection {
    
    private static String getH2Url(Melonade melonade) {
        return String.format("%smem:%s", Constants.START_URL, melonade.getId());
    }
    
    private static java.util.Properties applyDefaultConnectionSettings(Properties properties) {
        properties.put("AUTOCOMMIT", "false");
        return properties.legacy();
    }
    
    protected Melonade melonade;
    
    public MelonConnection(Melonade melonade) throws SQLException {
        super(getH2Url(melonade), applyDefaultConnectionSettings(melonade.getProperties()));
        
        this.melonade = melonade;
        this.melonade.syncToDatabase(this);
    }

    
    @Override
    public synchronized void close() throws SQLException {
        super.close();
        
        MelonadeFactory.INSTANCE.remove(melonade);
    }
    
    /**
     * Commits the current transaction. This call has only an effect if auto
     * commit is switched off.
     *
     * @throws SQLException if the connection is closed
     */
    @Override
    public synchronized void commit() throws SQLException {
        super.commit();

        this.melonade.syncToStorage(this);
    }

    @Override
    public synchronized void rollback() throws SQLException {
        super.rollback();

        this.melonade.syncToDatabase(this);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        super.rollback(savepoint);

        this.melonade.syncToDatabase(this);
    }
}
