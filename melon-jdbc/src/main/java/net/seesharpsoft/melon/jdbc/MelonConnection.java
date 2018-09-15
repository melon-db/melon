package net.seesharpsoft.melon.jdbc;

import lombok.Getter;
import net.seesharpsoft.melon.MelonInfo;
import net.seesharpsoft.melon.Melonade;
import org.h2.engine.Constants;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

public class MelonConnection extends org.h2.jdbc.JdbcConnection {
    
    private static String getH2Url(MelonInfo melonInfo) {
        return String.format("%smem:%s", Constants.START_URL, melonInfo.getUuid());
    }
    
    private static Properties applyDefaultConnectionSettings(Properties properties) {
        properties.setProperty("AUTOCOMMIT", "false");
        return properties;
    }
    
    @Getter
    private MelonInfo melonInfo;
    
    @Getter
    private int melonSyncCounter;
    
    public MelonConnection(MelonInfo melonInfo) throws SQLException {
        super(getH2Url(melonInfo), applyDefaultConnectionSettings(melonInfo.getProperties()));
        
        this.melonInfo = melonInfo;

        syncMelonToH2();
    }

    private void syncMelonToH2() throws SQLException {
        if (melonSyncCounter == 0) {
            try {
                ++melonSyncCounter;
                Melonade.syncToDatabase(this);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                --melonSyncCounter;
            }
        }
    }

    private void syncH2ToMelon() throws SQLException {
        if (melonSyncCounter == 0) {
            try {
                ++melonSyncCounter;
                Melonade.syncToStorage(this);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                --melonSyncCounter;
            }
        }
    }

    @Override
    public synchronized void close() throws SQLException {
        super.close();
        
        Melonade.close(this);
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

        syncH2ToMelon();
    }

    @Override
    public synchronized void rollback() throws SQLException {
        super.rollback();

        syncMelonToH2();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        super.rollback(savepoint);

        syncMelonToH2();
    }
}
