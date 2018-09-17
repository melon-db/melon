package net.seesharpsoft.melon.test;

import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.MelonHelper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestHelper {
    
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
    
}
