package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;

import java.util.ArrayList;
import java.util.List;

public class MelonConfig extends ConfigBase {

    public List<SchemaConfig> schemas;

    public MelonConfig() {
        schemas = new ArrayList<>();
        properties = new Properties();
    }

}
