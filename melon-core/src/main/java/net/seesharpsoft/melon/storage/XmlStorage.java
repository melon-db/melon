package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.FileStorageBase;

import javax.xml.stream.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class XmlStorage extends FileStorageBase {
    public static final String PROPERTY_ROOT = "path";

    public static final String PROPERTY_INDENT = "indent";
    
    public static final String PROPERTY_LINEBREAK = "linebreak";
    
    private static final String SPACE_STRING = "                                ";

    public XmlStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        String currentPath = "";
        String rootPath = properties.get(PROPERTY_ROOT);
        boolean readValue = false;

        List<List<String>> result = new ArrayList<>();
        List<String> currentValues = null;
        try (FileInputStream fis = new FileInputStream(file)) {
            XMLInputFactory xmlInFact = XMLInputFactory.newInstance();
            XMLStreamReader reader = xmlInFact.createXMLStreamReader(fis);
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
                    readValue = true;
                }
                if (reader.isStartElement() && !wasInRootPath && isInRootPath) {
                    currentValues = new ArrayList<>();
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
        String indent = createIndent(properties.getOrDefault(PROPERTY_INDENT, 4));
        String linebreak = properties.getOrDefault(PROPERTY_LINEBREAK, "\n");
        int level = 0;
        int columnSize = table.getColumns().size();

        try (FileOutputStream fos = new FileOutputStream(file)) {
            XMLOutputFactory xmlOutFact = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = xmlOutFact.createXMLStreamWriter(fos);
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
                ++level;
                linebreakAndIndent(writer, indent, level, linebreak);
                
                for (int i = 0; i < columnSize; ++i) {
                    Column column = table.getColumns().get(i);

                    writer.writeStartElement(column.getName());
                    writer.writeCharacters(values.size() <= i ? "" : values.get(i));
                    writer.writeEndElement();
                    if (i == columnSize - 1) {
                        --level;
                    }
                    linebreakAndIndent(writer, indent, level, linebreak);
                }
                writer.writeEndElement();
                if (records.indexOf(values) == records.size() - 1) {
                    --level;
                }
                linebreakAndIndent(writer, indent, level, linebreak);
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
