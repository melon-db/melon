package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

public abstract class FileStorageBase extends StorageBase {

    public static final String PROPERTY_ENCODING = "storage-encoding";

    public static final Charset DEFAULT_ENCODING = StandardCharsets.UTF_8;

    protected File file;

    public FileStorageBase(Table table, Properties properties, File file) throws IOException {
        super(table, properties);
        Objects.requireNonNull(file, "file must not be null!");
        setFile(file);
    }

    protected void setFile(File file) throws IOException {
        this.file = file;
        if (!file.exists()) {
            file.createNewFile();
        }
    }

    protected Charset getEncoding() {
        String charsetName = this.properties.get(PROPERTY_ENCODING);
        return charsetName == null ? DEFAULT_ENCODING : Charset.forName(charsetName);
    }

    protected Reader getReader() throws FileNotFoundException, UnsupportedEncodingException {
        return getReader(this.file);
    }

    protected Reader getReader(File file) throws FileNotFoundException, UnsupportedEncodingException {
        return new InputStreamReader(new FileInputStream(file.getAbsolutePath()), getEncoding());
    }

    protected Writer getWriter() throws FileNotFoundException, UnsupportedEncodingException {
        return getWriter(this.file);
    }

    protected Writer getWriter(File file) throws FileNotFoundException, UnsupportedEncodingException {
        return new OutputStreamWriter(new FileOutputStream(file.getAbsolutePath()), getEncoding());
    }

    @Override
    public long getLastModified() {
        return file.lastModified();
    }

    @Override
    protected long getSyncTime() {
        return file == null ? Long.MAX_VALUE : file.lastModified();
    }

    protected List<List<String>> read(Table table, Properties properties) throws IOException {
        return read(file, table, properties);
    }

    protected void write(Table table, Properties properties, List<List<String>> records) throws IOException {
        write(file, table, properties, records);
    }

    protected abstract List<List<String>> read(File file, Table table, Properties properties) throws IOException;

    protected abstract void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException;
}
