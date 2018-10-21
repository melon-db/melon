package net.seesharpsoft.melon.storage;

import net.seesharpsoft.melon.Constants;
import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.jdbc.MelonDriver;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MelonStorageUT extends TestFixture {

    public static void assertData(Connection connection) throws SQLException {
        try(ResultSet rs = connection.prepareStatement("SELECT * FROM CustomerWithCountry ORDER BY email").executeQuery()) {
            rs.next();
            assertThat(rs.getString("email"), is("arandal@att.net"));
            assertThat(rs.getString("fName"), is("Arandal"));
            assertThat(rs.getString("lastName"), is("Matthew"));
            assertThat(rs.getString("countryCode"), is("IS"));
            assertThat(rs.getString("countryName"), is("Iceland"));
            rs.next();
            assertThat(rs.getString("email"), is("danzigism@icloud.com"));
            assertThat(rs.getString("fName"), is("Daniel"));
            assertThat(rs.getString("lastName"), is("Ziggy"));
            assertThat(rs.getString("countryCode"), is("NE"));
            assertThat(rs.getString("countryName"), is("Niger"));
            rs.next();
            assertThat(rs.getString("email"), is("mddallara@yahoo.com"));
            assertThat(rs.getString("fName"), is("Madam"));
            assertThat(rs.getString("lastName"), is("Dallara"));
            assertThat(rs.getString("countryCode"), is("US"));
            assertThat(rs.getString("countryName"), is("United States"));

            assertThat(rs.next(), is(false));
        }
    }

    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/melonstorage/config.yaml",
                "/melonstorage/countries.properties",
                "/melonstorage/customer.xml"
        };
    }

    @Test
    public void melon_storage_should_be_created() throws SQLException {
        java.util.Properties properties = new java.util.Properties();
        properties.put(Constants.PROPERTY_CONFIG_FILE, "/melonstorage/config.yaml");
        try (Connection connection = DriverManager.getConnection(String.format("%sh2:mem:memdb", MelonDriver.MELON_URL_PREFIX), properties)) {
            assertData(connection);
        }
    }

    @Test
    public void melon_storage_changes_should_be_stored() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/melonstorage/config.yaml")) {
            connection.prepareStatement("UPDATE CustomerWithCountry SET fname = 'Tobi', lastName = 'Tester', countryName = 'Test Country' WHERE email = 'danzigism@icloud.com'").execute();
            connection.commit();

            Storage storage = connection.getMelon().getSchema().getTable("Customer").getStorage();
            List<List<String>> records = storage.read();
            assertThat(records.stream().filter(record -> record.get(0).equals("danzigism@icloud.com")).findFirst().orElse(null), is(Arrays.asList("danzigism@icloud.com", "Tobi", "Tester", "NE")));

            storage = connection.getMelon().getSchema().getTable("Country").getStorage();
            records = storage.read();
            assertThat(records.stream().filter(record -> record.get(0).equals("NE")).findFirst().orElse(null), is(Arrays.asList("NE", "Test Country")));
        }
    }
}
