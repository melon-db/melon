package net.seesharpsoft.melon.impl;

import lombok.Getter;
import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.View;

public class ViewImpl implements View {
    
    @Getter
    private final String name;
    
    @Getter
    private final String query;

    @Getter
    private final Properties properties;
    
    public ViewImpl(String name, String query, Properties properties) {
        this.name = name;
        this.query = query;
        this.properties = new Properties(properties);
    }
}
