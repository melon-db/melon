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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class XmlStorage extends FileStorageBase {
    public static final String PROPERTY_ROOT = "path";

    public XmlStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        String currentPath = "";
        String rootPath = properties.get(PROPERTY_ROOT);
//        boolean isInRootPath = false;
        boolean readValue = false;

        List<List<String>> result = new ArrayList<>();
        List<String> currentValues = null;
        try (FileInputStream fis = new FileInputStream(file)) {
            XMLInputFactory xmlInFact = XMLInputFactory.newInstance();
            XMLStreamReader reader = xmlInFact.createXMLStreamReader(fis);
            while (reader.hasNext()) {
                reader.next(); // do something here

                if (readValue) {
                    currentValues.add(reader.getText());
                    readValue = false;
                    continue;
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

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        String root = properties.get(PROPERTY_ROOT);
        String[] paths = root.split("/");

        try (FileOutputStream fos = new FileOutputStream(file)) {
            XMLOutputFactory xmlOutFact = XMLOutputFactory.newInstance();
            XMLStreamWriter writer = xmlOutFact.createXMLStreamWriter(fos);
            writer.writeStartDocument();

            for (int i = 0; i < paths.length - 1; ++i) {
                if (!paths[i].isEmpty()) {
                    writer.writeStartElement(paths[i]);
                }
            }

            for (List<String> values : records) {
                writer.writeStartElement(paths[paths.length - 1]);
                for (int i = 0; i < table.getColumns().size(); ++i) {
                    Column column = table.getColumns().get(i);

                    writer.writeStartElement(column.getName());
                    writer.writeCharacters(values.size() <= i ? "" : values.get(i));
                    writer.writeEndElement();
                }
                writer.writeEndElement();
            }

            for (int i = 0; i < paths.length - 1; ++i) {
                if (!paths[i].isEmpty()) {
                    writer.writeEndElement();
                }
            }

            writer.writeEndDocument();

            writer.flush();
            writer.close();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }
}
