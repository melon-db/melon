package net.seesharpsoft.melon;

import lombok.Getter;
import lombok.Setter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.sql.SqlHelper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class Melon {

    @Getter
    private final String url;

    @Getter
    private final Schema schema;

    @Getter
    private Properties properties;

    @Getter
    @Setter
    private boolean initialized = false;

    @Getter
    @Setter
    private int referenceCounter = 0;

    protected int melonSyncCounter;

    public Melon(String url, Schema schema, Properties properties) {
        this.url = url;
        this.schema = schema;
        this.properties = new Properties(properties);
    }

    protected boolean tableExists(Connection connection, Table table) throws SQLException {
        String dbTableName = SqlHelper.sanitizeDbName(table.getName());
        try (ResultSet resultSet = connection.getMetaData().getTables(null, null, dbTableName, null)) {
            return resultSet.next();
        }
    }

//    protected void clearDatabaseTable(Connection connection, Table table) throws SQLException {
//        clearDatabaseTable(connection, table, Collections.emptyList());
//    }

    protected void clearDatabaseTable(Connection connection, Table table, List<String> primaryValuesToKeep) throws SQLException {
        if (!tableExists(connection, table)) {
            return;
        }
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateClearTableStatement(table, primaryValuesToKeep))) {
            if (primaryValuesToKeep != null) {
                for (int i = 0; i < primaryValuesToKeep.size(); ++i) {
                    ps.setObject(i + 1, primaryValuesToKeep.get(i));
                }
            }
            ps.execute();
        }
    }

    protected void createSchema(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateCreateSchemaStatement(getSchema()))) {
            ps.execute();
        }
    }

    protected void syncToDatabase(Connection connection, Table table, List<List<String>> records) throws SQLException {
        clearDatabaseTable(connection, table, table.getPrimaryValues(records));
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateMergeStatement(table))) {
            final int batchSize = 1000;
            int count = 0;
            int columnSize = table.getColumns().size();

            for (List<String> fields : records) {
                for (int i = 0; i < columnSize; ++i) {
                    ps.setString(i + 1, fields.size() <= i ? null : fields.get(i));
                }
                ps.addBatch();

                if (++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
        }
        connection.commit();
    }

    public void syncToDatabase(Connection connection, boolean forceSync) throws SQLException {
        if (melonSyncCounter == 0) {
            try {
                ++melonSyncCounter;

                initializeMelon(connection);

                for (Table table : getSchema().getTables()) {
                    Storage storage = table.getStorage();
                    if (storage == null) {
                        throw new RuntimeException("no storage for " + table);
                    }
                    if (storage.isDirty() || forceSync) {
                        try {
                            syncToDatabase(connection, table, storage.read());
                        } catch(SQLException sqlException) {
                            storage.isDirty(true);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SQLException sqlException) {
                setInitialized(false);
                throw sqlException;
            } finally {
                --melonSyncCounter;
            }
        }
    }

    protected void initializeMelon(Connection connection) throws SQLException {
        if (isInitialized()) {
            return;
        }
        createSchema(connection);
        connection.setSchema(SqlHelper.sanitizeDbName(getSchema().getName()));
        for (Table table : getSchema().getTables()) {
            createDatabaseSchemaTable(connection, table);
        }
        for (View view : getSchema().getViews()) {
            createDatabaseSchemaView(connection, view);
        }
        setInitialized(true);
    }

    protected static void createDatabaseSchemaTable(Connection connection, Table table) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateCreateTableStatement(table))) {
            ps.execute();
        }
    }

    protected static void createDatabaseSchemaView(Connection connection, View view) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateCreateViewStatement(view))) {
            ps.execute();
        }
    }

    public void syncToStorage(Connection connection) throws SQLException {
        if (melonSyncCounter == 0) {
            try {
                ++melonSyncCounter;
                for (Table table : getSchema().getTables()) {
                    Storage storage = table.getStorage();
                    syncToStorage(connection, table, storage);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                --melonSyncCounter;
            }
        }
    }

    protected static void syncToStorage(Connection connection, Table table, Storage storage) throws SQLException, IOException {
        List<List<String>> records = null;
        try (PreparedStatement ps = connection.prepareStatement(SqlHelper.generateSelectStatement(table))) {
            try (ResultSet rs = ps.executeQuery()) {
                records = SqlHelper.fromResultSet(rs);
            }
        }
        storage.write(records);
    }
}
