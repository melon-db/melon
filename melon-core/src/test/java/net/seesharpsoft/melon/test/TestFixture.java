package net.seesharpsoft.melon.test;

import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.Constants;
import net.seesharpsoft.melon.MelonHelper;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.jdbc.MelonDriver;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public abstract class TestFixture {
    
    public abstract String[] getResourceFiles();

    public static void createBackupFiles(String[] fileNames) throws IOException {
        for (String fileName : fileNames) {
            File testFile = MelonHelper.getFile(fileName);
            File backupFile = new File(testFile.getAbsolutePath() + "_BACKUP");
            if (!backupFile.exists()) {
                backupFile.createNewFile();
            }
            try(FileWriter writer = new FileWriter(backupFile)) {
                writer.write(SharpIO.readAsString(testFile.getAbsolutePath()));
                writer.flush();
            }
        }
    }

    public static void restoreBackupFiles(String[] fileNames) throws IOException {
        for (String fileName : fileNames) {
            try(FileWriter writer = new FileWriter(MelonHelper.getFile(fileName))) {
                writer.write(SharpIO.readAsString(MelonHelper.getFile(fileName + "_BACKUP").getAbsolutePath()));
                writer.flush();
            }
        }
    }

    protected MelonConnection getConnection(String fileName) throws SQLException {
        Properties info = new Properties();
        info.put(Constants.PROPERTY_CONFIG_FILE, fileName);
        info.put("AUTOCOMMIT", "false");
        return (MelonConnection) DriverManager.getConnection(String.format("%sh2:mem:%s", MelonDriver.MELON_URL_PREFIX, "test"), info);
    }
    
    @Before
    public void beforeEach() throws IOException {
        createBackupFiles(getResourceFiles());
    }

    @After
    public void afterEach() throws IOException {
        restoreBackupFiles(getResourceFiles());
    }
}
