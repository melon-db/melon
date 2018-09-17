package net.seesharpsoft.melon.html;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.FileStorageBase;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HtmlStorage extends FileStorageBase {

    // public static final String PROPERTY_

    public HtmlStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        List<List<String>> result = new ArrayList<>();
        Document htmlDocument = Jsoup.parse(file, getCharset());
        for (Element element : htmlDocument.body().children()) {
            List<String> values = new ArrayList<>();
            values.add(element.attr("index"));
            Element actualEntry = element.child(0);
            values.add(actualEntry.text());
            result.add(values);
        }
        return result;
    }

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        throw new NotImplementedException();
    }
}
