package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.MelonHelper;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class HtmlStorageUT extends TestFixture {

    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/Simple.yaml",
                "/data/SimpleText.html"
        };
    }
    
    @Test
    public void should_parse_fieldInfo_correctly() throws SQLException {
        try (MelonConnection connection = getConnection("/Simple.yaml")) {
            Map<String, String> map = connection.getMelon().getSchema().getTable("Text").getStorage().getProperties().getOrDefault(HtmlStorage.PROPERTY_COLUMN_ATTRIBUTES, null);
            
            assertThat(map.size(), is(3));
            assertThat(map.get("type"), is("field"));
            assertThat(map.get("index"), is("$index$"));
            assertThat(map.get("name"), is("$name$"));
        }
    }

    @Test
    public void should_parse_html() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/Simple.yaml")) {
            Storage storage = connection.getMelon().getSchema().getTable("Text").getStorage();
            
            List<List<String>> entries = storage.read();
            
            assertThat(entries.size(), is(3));
            assertThat(entries.get(0), is(Arrays.asList("0", "Title A", "Text A")));
            assertThat(entries.get(1), is(Arrays.asList("1", "Title B", "Text B")));
            assertThat(entries.get(2), is(Arrays.asList("2", "1 to 10", "One to ten")));
        }
    }

    @Test
    public void should_write_html() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/Simple.yaml")) {
            Storage storage = connection.getMelon().getSchema().getTable("Text").getStorage();

            List<List<String>> entries = storage.read();

            storage.write(entries);
            
            assertThat(SharpIO.readAsString(MelonHelper.getFile("/data/SimpleText.html").getAbsolutePath()),
                    is(SharpIO.readAsString(MelonHelper.getFile("/results/SimpleText.html").getAbsolutePath())));
        }
    }
    
}
