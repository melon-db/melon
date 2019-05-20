package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class XlsStorageUT extends TestFixture {

    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/SimpleFile.yaml",
                "/data/SimpleFile.xlsx",
                "/SimpleNewFile.yaml",
                "/data/SimpleNewFile.xlsx"
        };
    }

    @Test
    public void should_read_xlsx() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/SimpleFile.yaml")) {
            Storage storage = connection.getMelon().getSchema().getTable("Text").getStorage();

            List<List<String>> entries = storage.read();

            assertThat(entries.size(), is(2));
            assertThat(entries.get(0), is(Arrays.asList("id 1", "Title 1", "Text 1")));
            assertThat(entries.get(1), is(Arrays.asList("id 2", "Title 2", "Text 2")));
        }
    }

    @Test
    public void should_create_new_xlsx() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/SimpleNewFile.yaml")) {
            Storage storage = connection.getMelon().getSchema().getTable("Text").getStorage();

            List<List<String>> entries = new ArrayList<>();

            entries.add(Arrays.asList("id 1", "Title 1", "Text 1"));
            entries.add(Arrays.asList("id 2", "Title 2", "Text 2"));

            storage.write(entries);

            assertThat(
                    Files.exists(SharpIO.getFile("/data/SimpleNewFile.xlsx").toPath()),
                    is(true));
        }
    }
}
