package net.seesharpsoft.melon.config;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.View;
import net.seesharpsoft.melon.impl.ViewImpl;

public class ViewConfig extends ConfigBase {
    
    public String name;
    
    public String query;
    
    public View getView(Properties additionalProperties) {
        return new ViewImpl(name, query, getProperties(additionalProperties));
    }
}
