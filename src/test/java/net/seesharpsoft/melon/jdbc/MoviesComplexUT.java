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
            connection.prepareStatement("INSERT INTO Movie_Full (IDENTIFIER) VALUES ('Test')").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("Movie").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.size(), is(1));
        }
    }


    @Test
    public void should_insert_new_entry_in_empty_file_and_update_referenced_values() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/MoviesComplex.yaml")) {
            connection.prepareStatement("INSERT INTO Movie_Full (IDENTIFIER, COUNTRYCODE) VALUES ('Test', 'AD')").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("Movie_Full").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.size(), is(1));
            assertThat(records.get(0).get(2), is("AD"));
            assertThat(records.get(0).get(3), is("Andorra"));
        }
    }

}
