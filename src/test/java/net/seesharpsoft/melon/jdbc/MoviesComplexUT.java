package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MoviesComplexUT extends TestFixture {
    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/schemas/MoviesComplex.yaml",
                "/schemas/movies.xml",
                "/schemas/movies_en.properties",
                "/schemas/movies_de.properties",
                "/data/countries.properties"
        };
    }

    @Test
    public void should_insert_new_entry_in_empty_file() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/MoviesComplex.yaml")) {
            connection.prepareStatement("INSERT INTO \"Movie_Full\" (\"Identifier\") VALUES ('Test')").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("Movie").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.size(), is(1));
        }
    }

    @Test
    public void should_insert_new_entry_in_empty_file_with_reference_value() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/MoviesComplex.yaml")) {
            connection.prepareStatement("INSERT INTO \"Movie_Full\" (\"Identifier\", \"CountryCode\") VALUES ('Test', 'AD')").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("Movie_Full").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.size(), is(1));
            assertThat(records.get(0).get(2), is("AD"));
            assertThat(records.get(0).get(3), is("Andorra"));
        }
    }


    @Test
    public void should_insert_new_entry_in_empty_file_and_create_reference_value() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/MoviesComplex.yaml")) {
            connection.prepareStatement("INSERT INTO \"Movie_Full\" (\"Identifier\", \"Name_en\") VALUES ('Test', 'Test English')").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("Movie_Full").getStorage();
            List<List<String>> records = storage.read();
            assertThat(records.size(), is(1));
            assertThat(records.get(0).get(4), is("Test English"));

            storage = connection.melon.getSchema().getTable("Movie_en").getStorage();
            records = storage.read();

            assertThat(records.size(), is(1));
            assertThat(records.get(0).get(0), is("Test"));
            assertThat(records.get(0).get(1), is("Test English"));
        }
    }

    @Test
    public void should_update_new_entry_in_empty_file_and_create_reference_value() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/MoviesComplex.yaml")) {
            connection.prepareStatement("INSERT INTO \"Movie_Full\" (\"Identifier\", \"Name_en\") VALUES ('Test', 'Test English')").execute();
            connection.commit();

            connection.prepareStatement("UPDATE \"Movie_Full\" SET \"Name_en\" = 'Test English Updated', \"Name_de\" = 'Test German' WHERE \"Identifier\" = 'Test'").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("Movie_Full").getStorage();
            List<List<String>> records = storage.read();
            assertThat(records.size(), is(1));
            assertThat(records.get(0).get(4), is("Test English Updated"));
            assertThat(records.get(0).get(5), is("Test German"));

            storage = connection.melon.getSchema().getTable("Movie_en").getStorage();
            records = storage.read();
            assertThat(records.size(), is(1));
            assertThat(records.get(0).get(0), is("Test"));
            assertThat(records.get(0).get(1), is("Test English Updated"));

            storage = connection.melon.getSchema().getTable("Movie_de").getStorage();
            records = storage.read();
            assertThat(records.size(), is(1));
            assertThat(records.get(0).get(0), is("Test"));
            assertThat(records.get(0).get(1), is("Test German"));
        }
    }
}
