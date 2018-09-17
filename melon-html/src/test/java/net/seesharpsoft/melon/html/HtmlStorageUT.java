package net.seesharpsoft.melon.html;

import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.MelonHelper;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.jdbc.MelonadeDriver;
import net.seesharpsoft.melon.test.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HtmlStorageUT {

    private static final String[] TEST_FILES = new String[] {
            "/Simple.yaml",
            "/data/SimpleText.html"
    };

    @Before
    public void beforeEach() throws IOException {
        TestHelper.createBackupFiles(TEST_FILES);
        MelonadeDriver.load();
    }

    @After
    public void afterEach() throws IOException {
        TestHelper.restoreBackupFiles(TEST_FILES);
    }
    
    @Test
    public void should_parse_fieldInfo_correctly() throws SQLException {
        try (Connection connection = DriverManager.getConnection(String.format("%s/Simple.yaml", MelonadeDriver.MELON_STANDALONE_URL_PREFIX))) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            Map<String, String> map = melonConnection.getMelon().getSchema().getTable("Text").getStorage().getProperties().getOrDefault(HtmlStorage.PROPERTY_COLUMN_ATTRIBUTES, null);
            
            assertThat(map.size(), is(3));
            assertThat(map.get("type"), is("field"));
            assertThat(map.get("index"), is("$index$"));
            assertThat(map.get("name"), is("$name$"));
        }
    }

    @Test
    public void should_parse_html() throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection(String.format("%s/Simple.yaml", MelonadeDriver.MELON_STANDALONE_URL_PREFIX))) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            Storage storage = melonConnection.getMelon().getSchema().getTable("Text").getStorage();
            
            List<List<String>> entries = storage.read();
            
            assertThat(entries.size(), is(3));
            assertThat(entries.get(0), is(Arrays.asList("0", "Title A", "Text A")));
            assertThat(entries.get(1), is(Arrays.asList("1", "Title B", "Text B")));
            assertThat(entries.get(2), is(Arrays.asList("2", "1 to 10", "One to ten")));
        }
    }

    @Test
    public void should_write_html() throws SQLException, IOException {
        try (Connection connection = DriverManager.getConnection(String.format("%s/Simple.yaml", MelonadeDriver.MELON_STANDALONE_URL_PREFIX))) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            Storage storage = melonConnection.getMelon().getSchema().getTable("Text").getStorage();

            List<List<String>> entries = storage.read();

            storage.write(entries);
            
            assertThat(SharpIO.readAsString(MelonHelper.getFile("/data/SimpleText.html").getAbsolutePath()),
                    is(SharpIO.readAsString(MelonHelper.getFile("/results/SimpleText.html").getAbsolutePath())));
        }
    }
    
}
