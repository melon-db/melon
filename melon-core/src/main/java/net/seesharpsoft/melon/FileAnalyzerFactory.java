package net.seesharpsoft.melon;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.commons.util.SharpIO;
import net.seesharpsoft.melon.config.TableConfig;

import java.util.*;

import static net.seesharpsoft.melon.StorageFactory.MELON_STORAGE_PROTOCOL;

public class FileAnalyzerFactory {

    private static ServiceLoader<FileAnalyzer> adapterServiceLoader = ServiceLoader.load(FileAnalyzer.class);

    /**
     * Singleton.
     */
    public static final FileAnalyzerFactory INSTANCE = new FileAnalyzerFactory();

    public Collection<FileAnalyzer> getAnalyzersFor(String uri) {
        Object input = getAnalyzerInput(uri);

        List<FileAnalyzer> fileAnalyzers = new ArrayList<>();
        FileAnalyzer fileAnalyzer = null;
        Iterator<FileAnalyzer> fileAnalyzerIterator = adapterServiceLoader.iterator();
        while (fileAnalyzerIterator.hasNext()) {
            fileAnalyzer = fileAnalyzerIterator.next();
            if (fileAnalyzer.canHandle(input)) {
                fileAnalyzers.add(fileAnalyzer);
            }
        }
        return fileAnalyzers;
    }

    public TableConfig analyze(String uri, Properties properties) {
        Object input = getAnalyzerInput(uri);
        Properties currentProperties = new Properties();
        if (properties != null) {
            currentProperties.putAll(properties);
        }

        TableConfig tableConfig = null;
        Collection<FileAnalyzer> fileAnalyzers = getAnalyzersFor(uri);
        for (FileAnalyzer fileAnalyzer : fileAnalyzers) {
            tableConfig = fileAnalyzer.analyze(input, currentProperties);
            if (tableConfig != null) {
                break;
            }
        }

        return tableConfig;
    }

    protected Object getAnalyzerInput(String uri) {
        if (uri == null || uri.trim().isEmpty()) {
            return null;
        }
        if (uri.startsWith(String.format("%s://", MELON_STORAGE_PROTOCOL))) {
            throw new UnsupportedOperationException(String.format("protocol '%s' not supported", MELON_STORAGE_PROTOCOL));
        }
        return SharpIO.getFile(uri);
    }
}
