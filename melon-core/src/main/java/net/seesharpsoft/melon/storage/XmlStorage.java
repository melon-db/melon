package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;

import javax.xml.stream.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XmlStorage extends FileStorageBase {
    public static final String PROPERTY_ROOT = "xml-record-path";

    public static final String PROPERTY_INDENT = "xml-indent";
    public static final int DEFAULT_INDENT = 4; // max: 32

    public static final String DEFAULT_LINEBREAK = "\n";

    public static final String PROPERTY_FORMAT = "xml-format";
    public static final String FORMAT_ELEMENTS = "Elements";
    public static final String FORMAT_ATTRIBUTES = "Attributes";
    public static final String DEFAULT_FORMAT = FORMAT_ELEMENTS;

    private static final String SPACE_STRING = "                                ";

    public XmlStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    protected String linebreak() {
        return DEFAULT_LINEBREAK;
    }

    protected int indent() {
        return properties.getOrDefault(PROPERTY_INDENT, DEFAULT_INDENT);
    }

    protected boolean valuesAsElements() {
        return properties.getOrDefault(PROPERTY_FORMAT, DEFAULT_FORMAT).equals(FORMAT_ELEMENTS);
    }

    protected boolean valuesAsAttributes() {
        return properties.getOrDefault(PROPERTY_FORMAT, DEFAULT_FORMAT).equals(FORMAT_ATTRIBUTES);
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        String currentPath = "";
        String rootPath = properties.get(PROPERTY_ROOT);
        boolean readValue = false;

        List<Column> columns = table.getColumns();
        List<List<String>> result = new ArrayList<>();
        List<String> currentValues = null;
        try (FileInputStream fis = new FileInputStream(file)) {
            XMLInputFactory xmlInFact = XMLInputFactory.newInstance();
            XMLStreamReader reader = xmlInFact.createXMLStreamReader(fis, getEncoding().name());
            while (reader.hasNext()) {
                reader.next(); // do something here

                if (readValue) {
                    readValue = false;
                    if (!reader.isCharacters()) {
                        currentValues.add(null);
                    } else {
                        currentValues.add(reader.getText());
                    }
                }

                boolean wasInRootPath = rootPath.equalsIgnoreCase(currentPath);

                if (reader.isStartElement()) {
                    currentPath += "/" + reader.getLocalName();
                }
                if (reader.isEndElement()) {

                    int lastSeparator = currentPath.lastIndexOf('/');
                    if (lastSeparator != -1) {
                        currentPath = currentPath.substring(0, lastSeparator);
                    }
                }
                boolean isInRootPath = rootPath.equalsIgnoreCase(currentPath);

                if (reader.isStartElement() && wasInRootPath) {
                    if (valuesAsElements()) {
                        readValue = true;
                    }
                }
                if (reader.isStartElement() && !wasInRootPath && isInRootPath) {
                    currentValues = new ArrayList<>();
                    if (valuesAsAttributes()) {
                        int maxAttributes = reader.getAttributeCount();
                        for (int i = 0; i < columns.size(); ++i) {
                            currentValues.add(maxAttributes <= i ? "" : reader.getAttributeValue(i));
                        }
                    }
                }
                if (reader.isEndElement() && wasInRootPath && !isInRootPath) {
                    result.add(currentValues);
                }
            }
            reader.close();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        return result;
    }

    private static void linebreakAndIndent(XMLStreamWriter writer, String indent, int level, String linebreak) throws XMLStreamException {
        writer.writeCharacters(linebreak);
        for (int i = 0; i < level; ++i) {
            writer.writeCharacters(indent);
        }
    }

    private static String createIndent(int length) {
        StringBuilder builder = new StringBuilder(SPACE_STRING);
        builder.setLength(Math.min(SPACE_STRING.length(), length));
        return builder.toString();
    }

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        String root = properties.get(PROPERTY_ROOT);
        String[] paths = root.split("/");
        String indent = createIndent(indent());
        String linebreak = linebreak();
        int level = 0;
        int columnSize = table.getColumns().size();

        try (FileOutputStream fos = new FileOutputStream(file)) {
            XMLOutputFactory xmlOutFact = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = xmlOutFact.createXMLStreamWriter(fos, getEncoding().name());
            writer.writeStartDocument();
            linebreakAndIndent(writer, indent, level, linebreak);

            for (int i = 0; i < paths.length - 1; ++i) {
                if (!paths[i].isEmpty()) {
                    writer.writeStartElement(paths[i]);
                    ++level;
                    linebreakAndIndent(writer, indent, level, linebreak);
                }
            }

            for (List<String> values : records) {
                writer.writeStartElement(paths[paths.length - 1]);
                if (valuesAsElements()) {
                    ++level;
                    linebreakAndIndent(writer, indent, level, linebreak);
                }

                for (int i = 0; i < columnSize; ++i) {
                    Column column = table.getColumns().get(i);

                    String value = values.size() <= i || values.get(i) == null ? "" : values.get(i);
                    if (valuesAsElements()) {
                        writer.writeStartElement(column.getName());
                        writer.writeCharacters(value);
                        writer.writeEndElement();
                        if (i == columnSize - 1) {
                            --level;
                        }
                        linebreakAndIndent(writer, indent, level, linebreak);
                    }
                    if (valuesAsAttributes()) {
                        writer.writeAttribute(column.getName(), value);
                    }
                }
                writer.writeEndElement();
                if (records.indexOf(values) == records.size() - 1) {
                    --level;
                }
                linebreakAndIndent(writer, indent, level, linebreak);

                writer.flush();

                fos.flush();
            }

            for (int i = 0; i < paths.length - 1; ++i) {
                if (!paths[i].isEmpty()) {
                    writer.writeEndElement();
                    --level;
                    linebreakAndIndent(writer, indent, level, linebreak);
                }
            }

            writer.writeEndDocument();

            writer.flush();
            writer.close();

            fos.flush();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }
}
