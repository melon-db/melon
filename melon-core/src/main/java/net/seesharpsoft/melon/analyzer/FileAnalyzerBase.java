package net.seesharpsoft.melon.analyzer;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.FileAnalyzer;
import net.seesharpsoft.melon.config.TableConfig;

import java.io.*;
import java.nio.charset.Charset;

import static net.seesharpsoft.melon.storage.FileStorageBase.DEFAULT_ENCODING;
import static net.seesharpsoft.melon.storage.FileStorageBase.PROPERTY_ENCODING;

public abstract class FileAnalyzerBase implements FileAnalyzer {

    @Override
    public final TableConfig analyze(Object input, Properties properties) {
        if (input instanceof File) {
            return analyze((File)input, properties);
        }
        return null;
    }

    @Override
    public final boolean canHandle(Object input) {
        if (input instanceof File) {
            return canHandle((File)input);
        }
        return false;
    }

    protected Charset getEncoding(Properties properties) {
        String charsetName = properties.get(PROPERTY_ENCODING);
        return charsetName == null ? DEFAULT_ENCODING : Charset.forName(charsetName);
    }

    protected Reader getReader(File file, Properties properties) throws FileNotFoundException {
        return new InputStreamReader(new FileInputStream(file.getAbsolutePath()), getEncoding(properties));
    }

    public abstract TableConfig analyze(File file, Properties properties);

    public abstract boolean canHandle(File file);
}
