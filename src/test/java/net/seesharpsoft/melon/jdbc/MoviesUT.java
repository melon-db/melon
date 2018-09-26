package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class MoviesUT extends TestFixture {
    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/schemas/movies.yaml",
                "/data/movies.xml",
                "/data/movies_title.properties",
                "/data/movies_title_de.properties",
                "/data/my_movies.csv"
        };
    }

    @Test
    public void should_insert_new_entry_in_empty_file() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/Movies.yaml")) {

            connection.prepareStatement("INSERT INTO Movie (NAME) VALUES ('Test')").execute();
            connection.commit();

        }
    }

}
