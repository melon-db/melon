package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.melon.Constants;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MelonDriverUT extends TestFixture {

    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/Simple.yaml",
                "/data/Address.xml"
        };
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
