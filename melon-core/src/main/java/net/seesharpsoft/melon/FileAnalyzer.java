package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.config.TableConfig;

public interface FileAnalyzer {
    boolean canHandle(Object input);

    TableConfig analyze(Object input, Properties properties);
}
