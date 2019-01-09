package net.seesharpsoft.melon.storage;

import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
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

    public static void assertData(ResultSet rs, List<List<String>> results) throws SQLException {
        for (List<String> record : results) {
            assertThat(rs.next(), is(true));

            for (int index = 0; index < record.size(); ++index) {
                assertEquals(record.get(index), rs.getString(index + 1));
            }
        }

        assertThat(rs.next(), is(false));
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
        try (Connection connection = getConnection("/melonstorage/config.yaml")) {
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

    @Test
    public void melon_storage_changes_should_update_referenced_tables() throws SQLException {
        try (MelonConnection connection = getConnection("/melonstorage/config.yaml")) {
            connection.prepareStatement("UPDATE CustomerWithCountry SET fname = 'Tobi', lastName = 'Tester', countryName = 'Test Country' WHERE email = 'danzigism@icloud.com'").execute();
            connection.commit();

            try(ResultSet rs = connection.prepareStatement("SELECT * FROM Country WHERE code = 'NE'").executeQuery()) {
                assertData(rs, Arrays.asList(
                        Arrays.asList("NE", "Test Country")
                ));
            }

            try(ResultSet rs = connection.prepareStatement("SELECT * FROM Customer WHERE email = 'danzigism@icloud.com'").executeQuery()) {
                assertData(rs, Arrays.asList(
                        Arrays.asList("danzigism@icloud.com", "Tobi", "Tester", "NE")
                ));
            }
        }
    }

    @Test
    public void melon_storage_changes_should_update_reference() throws SQLException {
        try (MelonConnection connection = getConnection("/melonstorage/config.yaml")) {
            connection.prepareStatement("UPDATE CustomerWithCountry SET fname = 'Tobi', lastName = 'Tester', countryCode = 'JP' WHERE email = 'danzigism@icloud.com'").execute();
            connection.commit();

            try(ResultSet rs = connection.prepareStatement("SELECT * FROM Country WHERE code IN ('NE', 'JP') ORDER BY code").executeQuery()) {
                assertData(rs, Arrays.asList(
                        Arrays.asList("JP", "Japan"),
                        Arrays.asList("NE", "Niger")
                ));
            }

            try(ResultSet rs = connection.prepareStatement("SELECT * FROM Customer WHERE email = 'danzigism@icloud.com'").executeQuery()) {
                assertData(rs, Arrays.asList(
                        Arrays.asList("danzigism@icloud.com", "Tobi", "Tester", "JP")
                ));
            }
        }
    }
}
