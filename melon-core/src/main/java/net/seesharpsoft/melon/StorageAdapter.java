package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;

import java.io.IOException;

public interface StorageAdapter extends Comparable {

    public static final int PRIORITY_LOWEST = Integer.MIN_VALUE;
    public static final int PRIORITY_DEFAULT = 0;
    public static final int PRIORITY_HIGHEST = Integer.MAX_VALUE;
    
    boolean canHandle(Table table, Properties properties, Object input);
    
    Storage createStorage(Table table, Properties properties, Object input) throws IOException;
    
    default int getPriority() {
        return PRIORITY_DEFAULT;
    }

    default int compareTo(Object other) {
        if (!(other instanceof StorageAdapter)) {
            throw new IllegalArgumentException("cannot compare with " + other);
        }
        int otherPriority = ((StorageAdapter)other).getPriority();
        return this.getPriority() > otherPriority ? -1 : this.getPriority() < otherPriority ? 1 : 0;
    }
        
}
