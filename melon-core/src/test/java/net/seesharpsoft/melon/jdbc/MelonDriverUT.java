package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.melon.Constants;
import net.seesharpsoft.melon.test.TestHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class MelonDriverUT {

    private static final String[] TEST_FILES = new String[] {
            "/Simple.yaml",
            "/data/Address.xml"
    };

    @Before
    public void beforeEach() throws IOException {
        TestHelper.createBackupFiles(TEST_FILES);
    }

    @After
    public void afterEach() throws IOException {
        TestHelper.restoreBackupFiles(TEST_FILES);
    }
    
    @Test
    public void should_connect_to_h2_mem() throws SQLException {
        Properties properties = new Properties();
        properties.put(Constants.PROPERTY_CONFIG_FILE, "/Simple.yaml");
        try (Connection connection = DriverManager.getConnection(String.format("%sh2:mem:memdb", MelonDriver.MELON_URL_PREFIX), properties)) {
            assertThat(connection, instanceOf(MelonConnection.class));

            ResultSet rs = connection.prepareStatement("SELECT * FROM Address ORDER BY ID").executeQuery();
            rs.next();
            assertThat(rs.getString("id"), is("1"));
            assertThat(rs.getString("city"), is("Las Vegas"));
            assertThat(rs.getString("country"), is("US"));
            rs.next();
            assertThat(rs.getString("id"), is("2"));
            assertThat(rs.getString("city"), is("Mexico City"));
            assertThat(rs.getString("country"), is("Mexico"));

            assertThat(rs.next(), is(false));
        }
    }
}
