package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.impl.TableImpl;
import net.seesharpsoft.melon.jdbc.MelonConnection;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MelonUT extends TestFixture {
    @Override
    public String[] getResourceFiles() {
        return new String[0];
    }

    @Test
    public void table_exists_should_return_true_if_table_exists() throws SQLException {
        try (MelonConnection connection = getConnection("/Country.yaml")) {
            Melon melon = connection.getMelon();
            assertThat(connection.getMelon().tableExists(connection, melon.getSchema().getTable("Country")), is(true));
        }
    }

    @Test
    public void table_exists_should_return_false_if_table_does_not_exists() throws SQLException {
        try (MelonConnection connection = getConnection("/Country.yaml")) {
            Melon melon = connection.getMelon();
            Table dummyTable = new TableImpl(melon.getSchema(), "Schlumpf", new Properties());
            assertThat(connection.getMelon().tableExists(connection, dummyTable), is(false));
        }
    }
}
