package net.seesharpsoft.melon.html;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.impl.ColumnImpl;
import net.seesharpsoft.melon.impl.TableImpl;
import net.seesharpsoft.melon.storage.XmlStorage;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class HtmlStorageUT {


    @Test
    public void should_parse_simple_html() throws IOException {
        TableImpl table = new TableImpl("Text", new Properties());
        ColumnImpl column = new ColumnImpl(table, "id", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "title", new Properties());
        table.addColumn(column);
        Properties properties = new Properties();
        HtmlStorage storage = new HtmlStorage(table, properties, new File(getClass().getResource("/data/SimpleText.html").getFile()));

        List<List<String>> data = storage.read();

        assertThat(data.size(), is(4));
    }

}
