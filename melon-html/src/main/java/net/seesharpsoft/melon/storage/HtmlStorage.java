package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.MelonHelper;
import net.seesharpsoft.melon.ReferenceType;
import net.seesharpsoft.melon.Table;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HtmlStorage extends FileStorageBase {

    public static final String PROPERTY_HEAD_CONTENT = "html-header";
    public static final String PROPERTY_FORMAT = "html-format";
    
    public static final String FORMAT_TABLE = "Table";
    public static final String FORMAT_LIST = "List";
    
    public static final String PROPERTY_COLUMN_ATTRIBUTES = "html-column-attributes";
    public static final String PROPERTY_RECORD_ATTRIBUTES = "html-record-attributes";

    public HtmlStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    protected String getFormat() {
        return getProperties().getOrDefault(PROPERTY_FORMAT, FORMAT_LIST);
    }
    
    protected String getHeader() {
        return getProperties().getOrDefault(PROPERTY_HEAD_CONTENT, null);
    }
    
    protected  List<List<String>> readDivFormat(Document htmlDocument, Table table) {
        List<Column> columns = table.getColumns();
        List<String> attributeColumns = getAttributeColumns(getRecordAttributes());

        List<List<String>> result = new ArrayList<>();
        for (Element element : htmlDocument.body().children()) {
            List<String> values = new ArrayList<>();
            int currentChildIndex = 0;
            for (Column column : columns) {
                if (attributeColumns.contains(column.getName())) {
                    values.add(element.attr(column.getName()));
                } else {
                    if (currentChildIndex < element.children().size()) {
                        values.add(element.child(currentChildIndex).text());
                    } else {
                        values.add(null);
                    }
                    ++currentChildIndex;
                }
            }
            result.add(values);
        }
        return result;
    }
    
    protected Map<String, String> getRecordAttributes() {
        return getProperties().getOrDefault(PROPERTY_RECORD_ATTRIBUTES, Collections.emptyMap());
    }

    protected Map<String, String> getColumnAttributes() {
        return getProperties().getOrDefault(PROPERTY_COLUMN_ATTRIBUTES, Collections.emptyMap());
    }

    protected List<String> getAttributeColumns(Map<String, String> recordAttributes) {
        final List<String> attributeColumns = new ArrayList<>();
        recordAttributes.forEach((key, value) -> {
            if (MelonHelper.getReferenceType(value) == ReferenceType.COLUMN) {
                attributeColumns.add(MelonHelper.getReferenceArgument(value));
            }
        });
        return attributeColumns;
    }
    
    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        Document htmlDocument = Jsoup.parse(file, getCharset());
        switch (getFormat()) {
            case FORMAT_LIST:
                return readDivFormat(htmlDocument, table);
            case FORMAT_TABLE:
                throw new NotImplementedException();
            default:
                throw new NotImplementedException();    
        }
    }

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        Document htmlDocument = Jsoup.parse(file, getCharset());
        htmlDocument.body().children().clear();
        String header = getHeader();
        if (header != null) {
            htmlDocument.head().children().clear();
            htmlDocument.head().html(header);
        }
        switch (getFormat()) {
            case FORMAT_LIST:
                writeDivFormat(htmlDocument, table, records);
                break;
            case FORMAT_TABLE:
                throw new NotImplementedException();
            default:
                throw new NotImplementedException();
        }

        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.write(htmlDocument.outerHtml());
            fileWriter.flush();
        }
    }

    private void writeDivFormat(Document htmlDocument, Table table, List<List<String>> records) {
        List<Column> columns = table.getColumns();
        Map<String, String> recordAttributes = getRecordAttributes();
        Map<String, String> columnAttributes = getColumnAttributes();
        List<String> attributeColumns = getAttributeColumns(recordAttributes);

        for (List<String> values : records) {
            Element recordElement = new Element("div");
            recordAttributes.forEach((key, value) -> {
                ReferenceType referenceType = MelonHelper.getReferenceType(value);
                if (referenceType != null) {
                    switch (referenceType) {
                        case INDEX:
                            recordElement.attr(key, String.valueOf(records.indexOf(values)));
                            break;
                        case COLUMN:
                            // handled separately
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
                
            });
            for (int i = 0; i < columns.size(); ++i) {
                final int index = i;
                Column column = columns.get(i);
                if (attributeColumns.contains(column.getName())) {
                    recordElement.attr(column.getName(), values.get(i));
                } else {
                    Element columnElement = new Element( "div");
                    columnAttributes.forEach((key, value) -> {
                        ReferenceType referenceType = MelonHelper.getReferenceType(value);
                        if (referenceType != null) {
                            switch (referenceType) {
                                case NAME:
                                    columnElement.attr(key, column.getName());
                                    break;
                                case INDEX:
                                    columnElement.attr(key, String.valueOf(index));
                                    break;
                                default:
                                    throw new NotImplementedException();
                            }
                        }
                        
                    });
                    recordElement.children().add(columnElement);
                }
            }            
            htmlDocument.body().children().add(recordElement);
        }
    }
}
