package net.seesharpsoft.melon;

import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.jdbc.MelonDriver;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class MelonadeUT {

    @After
    public void afterEach() {
        Melonade.reset();
    }
    
    @Test
    public void should_create_correct_config_object_from_yaml_and_initialise_schema() throws SQLException {
        try (Connection connection = DriverManager.getConnection(String.format("%s/schemas/UserOnlySchema.yaml", MelonDriver.MELON_URL_PREFIX))) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            assertThat(melonConnection.getMelonInfo().getProperties(), notNullValue());
        }
    }

    @Test
    public void should_create_correct_config_object_from_yaml_and_have_data_loaded() throws SQLException {
        try (Connection connection = DriverManager.getConnection(String.format("%s/schemas/UserOnlySchema.yaml", MelonDriver.MELON_URL_PREFIX))) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            ResultSet rs = melonConnection.prepareStatement("SELECT * FROM User ORDER BY ID").executeQuery();
            rs.next();
            assertThat(rs.getString("firstName"), is("Fritz"));
            assertThat(rs.getString("lastname"), is("Fuchs"));
            assertThat(rs.getString("ID"), is("1"));
            assertThat(rs.getString("additional"), nullValue());
            rs.next();
            assertThat(rs.getString("firstName"), is("Peter"));
            assertThat(rs.getString("lastname"), is("Pan"));
            assertThat(rs.getString("ID"), is("2"));
            assertThat(rs.getString("additional"), is("next"));

            assertThat(rs.next(), is(false));
        }
    }

    @Test
    public void should_not_persist_changes_in_source_file_if_not_committed() throws SQLException, IOException {
        try (MelonConnection connection = (MelonConnection) DriverManager.getConnection(String.format("%s/schemas/UserOnlySchema.yaml", MelonDriver.MELON_URL_PREFIX))) {
            connection.prepareStatement("UPDATE User SET firstname = 'Tobi', lastName = 'Tester' WHERE id = 1").execute();

            Storage storage = connection.getMelonInfo().getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.stream().filter(record -> record.get(0).equals("1")).findFirst().orElse(null), is(Arrays.asList("1", "Fritz", "Fuchs", null)));
        }
    }

    @Test
    public void should_not_persist_changes_in_source_file_on_rollback() throws SQLException, IOException {
        try (MelonConnection connection = (MelonConnection) DriverManager.getConnection(String.format("%s/schemas/UserOnlySchema.yaml", MelonDriver.MELON_URL_PREFIX))) {

            connection.prepareStatement("UPDATE User SET firstname = 'Tobi', lastName = 'Tester' WHERE id = 1").execute();
            connection.rollback();

            Storage storage = connection.getMelonInfo().getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.stream().filter(record -> record.get(0).equals("1")).findFirst().orElse(null), is(Arrays.asList("1", "Fritz", "Fuchs", null)));
        }
    }

    @Test
    public void should_persist_changes_in_source_file_on_commit() throws SQLException, IOException {
        try (MelonConnection connection = (MelonConnection) DriverManager.getConnection(String.format("%s/schemas/UserOnlySchema.yaml", MelonDriver.MELON_URL_PREFIX))) {

            connection.prepareStatement("UPDATE User SET firstname = 'Tobi', lastName = 'Tester' WHERE id = 1").execute();
            connection.commit();

            Storage storage = connection.getMelonInfo().getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.stream().filter(record -> record.get(0).equals("1")).findFirst().orElse(null), is(Arrays.asList("1", "Tobi", "Tester", null)));
        }
    }

    @Ignore
    @Test
    public void should_create_correct_config_object_from_absolute_file_path() throws SQLException {
        try (Connection connection = DriverManager.getConnection(String.format("%sD:/melon/melon-core/src/test/resources/schemas/UserOnlySchema.yaml", MelonDriver.MELON_URL_PREFIX))) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            assertThat(melonConnection.getMelonInfo().getProperties(), notNullValue());
        }
    }

    @Test
    public void should_create_connection_and_missing_storage_file() throws SQLException {
        try (Connection connection = DriverManager.getConnection(String.format("%s/schemas/UserAndGroupSchema.yaml", MelonDriver.MELON_URL_PREFIX))) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            assertThat(melonConnection.getMelonInfo().getProperties(), notNullValue());
        }
    }
}
