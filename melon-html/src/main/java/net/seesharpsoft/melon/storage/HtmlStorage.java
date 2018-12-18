package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.MelonHelper;
import net.seesharpsoft.melon.ReferenceType;
import net.seesharpsoft.melon.Table;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Entities;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

public class HtmlStorage extends FileStorageBase {

    public static final String PROPERTY_HEAD_CONTENT = "html-header";
    public static final String PROPERTY_FORMAT = "html-format";

    public static final String FORMAT_TABLE = "Table";
    public static final String FORMAT_LIST = "List";

    public static final String PROPERTY_COLUMN_ATTRIBUTES = "html-column-attributes";
    public static final String PROPERTY_RECORD_ATTRIBUTES = "html-record-attributes";

    public static final String PROPERTY_INDENT = "html-indent";
    public static final int DEFAULT_INDENT = 4; // max: 32

    public HtmlStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    protected String getFormat() {
        return getProperties().getOrDefault(PROPERTY_FORMAT, FORMAT_LIST);
    }

    protected String getHeader() {
        return getProperties().getOrDefault(PROPERTY_HEAD_CONTENT, null);
    }

    protected int getIndent() {
        return getProperties().getOrDefault(PROPERTY_INDENT, DEFAULT_INDENT);
    }

    protected  List<List<String>> readDivFormat(Document htmlDocument, Table table) {
        List<Column> columns = table.getColumns();
        Map<String, String> attributeColumns = getAttributeColumns(getRecordAttributes());

        List<List<String>> result = new ArrayList<>();
        for (Element element : htmlDocument.body().children()) {
            List<String> values = new ArrayList<>();
            int currentChildIndex = 0;
            for (Column column : columns) {
                if (attributeColumns.containsKey(column.getName())) {
                    values.add(element.attr(attributeColumns.get(column.getName())));
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

    protected Map<String, String> getAttributeColumns(Map<String, String> recordAttributes) {
        final Map<String, String> attributeColumns = new TreeMap<>();
        recordAttributes.forEach((key, value) -> {
            if (MelonHelper.getReferenceType(value) == ReferenceType.COLUMN) {
                attributeColumns.put(MelonHelper.getReferenceArgument(value), key);
            }
        });
        return attributeColumns;
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        Document htmlDocument = Jsoup.parse(file, getEncoding().name());
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
        Document htmlDocument = Jsoup.parse(file, getEncoding().name());
        htmlDocument.outputSettings(htmlDocument.outputSettings()
                .syntax(Document.OutputSettings.Syntax.html)
                .escapeMode(Entities.EscapeMode.base)
                .prettyPrint(true)
                .indentAmount(getIndent()));
        htmlDocument.body().empty();
        String header = getHeader();
        if (header != null) {
            htmlDocument.head().empty();
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

        try (Writer fileWriter = getWriter(file)) {
            fileWriter.write(htmlDocument.outerHtml());
            fileWriter.flush();
        }
    }

    private void writeDivFormat(Document htmlDocument, Table table, List<List<String>> records) {
        List<Column> columns = table.getColumns();
        Map<String, String> recordAttributes = getRecordAttributes();
        Map<String, String> columnAttributes = getColumnAttributes();
        Map<String, String> attributeColumns = getAttributeColumns(recordAttributes);

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
                } else {
                    recordElement.attr(key, value);
                }
            });
            for (int i = 0; i < columns.size(); ++i) {
                final int index = i;
                Column column = columns.get(i);
                String value = values.size() <= i || values.get(i) == null ? "" : values.get(i);
                if (attributeColumns.containsKey(column.getName())) {
                    recordElement.attr(attributeColumns.get(column.getName()), value);
                } else {
                    Element columnElement = new Element( "div");
                    columnAttributes.forEach((attributeName, attributeValue) -> {
                        ReferenceType referenceType = MelonHelper.getReferenceType(attributeValue);
                        if (referenceType != null) {
                            switch (referenceType) {
                                case NAME:
                                    columnElement.attr(attributeName, column.getName());
                                    break;
                                case INDEX:
                                    columnElement.attr(attributeName, String.valueOf(index));
                                    break;
                                default:
                                    throw new NotImplementedException();
                            }
                        } else {
                            columnElement.attr(attributeName, attributeValue);
                        }
                    });
                    columnElement.append(value);
                    recordElement.appendChild(columnElement);
                }
            }
            htmlDocument.body().appendChild(recordElement);
        }
    }
}
