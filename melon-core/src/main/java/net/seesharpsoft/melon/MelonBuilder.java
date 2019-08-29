package net.seesharpsoft.melon;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.config.*;

import java.util.ArrayList;

@Getter(AccessLevel.PROTECTED)
@Setter(AccessLevel.PROTECTED)
public class MelonBuilder {

    private MelonConfig melonConfig;

    public MelonBuilder() {
        melonConfig = new MelonConfig();
    }

    public SchemaBuilder addSchema(String name) {
        SchemaBuilder schemaBuilder = new SchemaBuilder(name);
        melonConfig.schemas.add(schemaBuilder.getSchemaConfig());
        return schemaBuilder;
    }

    protected Properties getProperties() {
        return getMelonConfig().getProperties();
    }

    public <T extends MelonBuilder> T property(String key, Object value) {
        getProperties().put(key, value);
        return (T)this;
    }

    public MelonConfig build() {
        return getMelonConfig();
    }

    public class SchemaBuilder extends MelonBuilder {

        @Getter
        @Setter(AccessLevel.PROTECTED)
        protected SchemaConfig schemaConfig;

        SchemaBuilder() {
            super();
            setMelonConfig(MelonBuilder.this.getMelonConfig());
        }

        SchemaBuilder(SchemaConfig theSchemaConfig) {
            this();
            setSchemaConfig(theSchemaConfig);
        }

        SchemaBuilder(String name) {
            this(new SchemaConfig());
            getSchemaConfig().name = name;
            getSchemaConfig().tables = new ArrayList<>();
            getSchemaConfig().properties = new Properties();
        }

        @Override
        protected Properties getProperties() {
            return getSchemaConfig().getProperties();
        }

        public TableBuilder addTable(String name) {
            TableBuilder tableBuilder = new TableBuilder(name);
            getSchemaConfig().tables.add(tableBuilder.getTableConfig());
            return tableBuilder;
        }

        public ViewBuilder addView(String name) {
            ViewBuilder viewBuilder = new ViewBuilder(name);
            getSchemaConfig().views.add(viewBuilder.getViewConfig());
            return viewBuilder;
        }

        public class ViewBuilder extends SchemaBuilder {

            @Getter
            @Setter(AccessLevel.PROTECTED)
            private ViewConfig viewConfig;

            ViewBuilder() {
                super();
                setSchemaConfig(SchemaBuilder.this.getSchemaConfig());
            }

            ViewBuilder(ViewConfig theViewConfig) {
                this();
                setViewConfig(theViewConfig);
            }

            ViewBuilder(String name) {
                this(new ViewConfig());
                getViewConfig().name = name;
                getViewConfig().properties = new Properties();
            }

            @Override
            protected Properties getProperties() {
                return getViewConfig().getProperties();
            }

            public ViewBuilder query(String query) {
                getViewConfig().query = query;
                return this;
            }
        }

        public class TableBuilder extends SchemaBuilder {

            @Getter
            @Setter(AccessLevel.PROTECTED)
            private TableConfig tableConfig;

            TableBuilder() {
                super();
                setSchemaConfig(SchemaBuilder.this.getSchemaConfig());
            }

            TableBuilder(TableConfig theTableConfig) {
                this();
                setTableConfig(theTableConfig);
            }

            TableBuilder(String name) {
                this(new TableConfig());
                getTableConfig().name = name;
                getTableConfig().columns = new ArrayList<>();
                getTableConfig().properties = new Properties();
            }

            @Override
            protected Properties getProperties() {
                return getTableConfig().getProperties();
            }

            public ColumnBuilder addColumn(String name) {
                ColumnBuilder columnBuilder = new ColumnBuilder(name);
                getTableConfig().columns.add(columnBuilder.getColumnConfig());
                return columnBuilder;
            }

            public StorageBuilder storage(String uri) {
                StorageBuilder storageBuilder = new StorageBuilder(uri);
                getTableConfig().storage = storageBuilder.getStorageConfig();
                return storageBuilder;
            }

            public class StorageBuilder extends TableBuilder {

                @Getter
                @Setter(AccessLevel.PROTECTED)
                private StorageConfig storageConfig;

                StorageBuilder() {
                    super();
                    setTableConfig(TableBuilder.this.getTableConfig());
                }

                StorageBuilder(StorageConfig theStorageConfig) {
                    this();
                    setStorageConfig(theStorageConfig);
                }

                StorageBuilder(String uri) {
                    this(new StorageConfig());
                    getStorageConfig().uri = uri;
                    getStorageConfig().properties = getProperties();
                }

                @Override
                protected Properties getProperties() {
                    return getStorageConfig().getProperties();
                }
            }

            public class ColumnBuilder extends TableBuilder {

                @Getter
                @Setter(AccessLevel.PROTECTED)
                private ColumnConfig columnConfig;

                ColumnBuilder() {
                    super();
                    setTableConfig(TableBuilder.this.getTableConfig());
                }

                ColumnBuilder(ColumnConfig theColumnConfig) {
                    this();
                    setColumnConfig(theColumnConfig);

                }

                ColumnBuilder(String name) {
                    this(new ColumnConfig());
                    getColumnConfig().name = name;
                    getColumnConfig().properties = getProperties();
                }

                @Override
                protected Properties getProperties() {
                    return getColumnConfig().getProperties();
                }

                public ColumnBuilder primary(boolean primary) {
                    getColumnConfig().primary = primary;
                    return this;
                }

                public ColumnBuilder source(String source) {
                    getColumnConfig().source = source;
                    return this;
                }

                public ColumnBuilder reference(String reference) {
                    getColumnConfig().reference = reference;
                    return this;
                }
            }
        }
    }
}
