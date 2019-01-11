package net.seesharpsoft.melon.jdbc;

import net.seesharpsoft.melon.Storage;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class MelonUT extends TestFixture {



    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/schemas/UserOnlySchema.yaml",
                "/data/Address.xml",
                "/data/Team.properties",
                "/data/User.csv"
        };
    }

    @Test
    public void should_create_correct_config_object_from_yaml_and_initialise_schema() throws SQLException {
        try (Connection connection = getConnection("/schemas/UserOnlySchema.yaml")) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            assertThat(melonConnection.melon.getProperties(), notNullValue());
        }
    }

    @Test
    public void should_create_correct_config_object_from_yaml_and_have_data_loaded() throws SQLException {
        try (Connection connection = getConnection("/schemas/UserOnlySchema.yaml")) {
            assertThat(connection, instanceOf(MelonConnection.class));

            ResultSet rs = connection.prepareStatement("SELECT * FROM \"User\" ORDER BY \"id\"").executeQuery();
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
        try (MelonConnection connection = getConnection("/schemas/UserOnlySchema.yaml")) {
            connection.prepareStatement("UPDATE \"User\" SET \"firstName\" = 'Tobi', \"lastName\" = 'Tester' WHERE \"id\" = 1").execute();

            Storage storage = connection.melon.getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.stream().filter(record -> record.get(0).equals("1")).findFirst().orElse(null), is(Arrays.asList("1", "Fritz", "Fuchs", null)));
        }
    }

    @Test
    public void should_not_persist_changes_in_source_file_on_rollback() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/UserOnlySchema.yaml")) {

            connection.prepareStatement("UPDATE \"User\" SET \"firstName\" = 'Tobi', \"lastName\" = 'Tester' WHERE \"id\" = 1").execute();
            connection.rollback();

            Storage storage = connection.melon.getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.stream().filter(record -> record.get(0).equals("1")).findFirst().orElse(null), is(Arrays.asList("1", "Fritz", "Fuchs", null)));
        }
    }

    // TODO cleanup
    @Test
    public void should_persist_changes_in_source_file_on_commit() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/UserOnlySchema.yaml")) {

            connection.prepareStatement("UPDATE \"User\" SET \"firstName\" = 'Tobi', \"lastName\" = 'Tester' WHERE \"id\" = 1").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.stream().filter(record -> record.get(0).equals("1")).findFirst().orElse(null), is(Arrays.asList("1", "Tobi", "Tester", null)));
        }
    }

    @Test
    public void should_not_persist_changes_in_source_if_accessMode_is_readOnly() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/SchemaAccessModeReadOnly.yaml")) {

            connection.prepareStatement("UPDATE \"User\" SET \"firstName\" = 'Tobi', \"lastName\" = 'Tester' WHERE \"id\" = 1").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.stream().filter(record -> record.get(0).equals("1")).findFirst().orElse(null), is(Arrays.asList("1", "Fritz", "Fuchs", null)));
        }
    }

    @Test
    public void should_persist_new_rows_on_commit() throws SQLException, IOException {
        try (MelonConnection connection = getConnection("/schemas/UserOnlySchema.yaml")) {

            connection.prepareStatement("INSERT INTO \"User\" (\"id\", \"firstName\", \"lastName\") VALUES ('3', 'Tobi', 'Tester')").execute();
            connection.commit();

            Storage storage = connection.melon.getSchema().getTable("User").getStorage();
            List<List<String>> records = storage.read();

            assertThat(records.size(), is(3));
            assertThat(records.stream().filter(record -> record.get(0).equals("3")).findFirst().orElse(null), is(Arrays.asList("3", "Tobi", "Tester", null)));
        }
    }

    @Ignore
    @Test
    public void should_create_correct_config_object_from_absolute_file_path() throws SQLException {
        try (Connection connection = getConnection("D:/melon/melon-core/src/test/resources/schemas/UserOnlySchema.yaml")) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            assertThat(melonConnection.melon.getProperties(), notNullValue());
        }
    }

    @Test
    public void should_create_connection_and_missing_storage_file() throws SQLException {
        try (Connection connection = getConnection("/schemas/UserAndTeamSchema.yaml")) {
            assertThat(connection, instanceOf(MelonConnection.class));

            MelonConnection melonConnection = (MelonConnection) connection;
            assertThat(melonConnection.melon.getProperties(), notNullValue());
        }
    }

    @Test
    public void should_create_connection() throws SQLException {
        try (Connection connection = getConnection("/schemas/All.yaml")) {
            assertThat(connection, instanceOf(MelonConnection.class));
        }
    }
}
