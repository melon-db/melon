package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.MelonHelper;
import net.seesharpsoft.melon.impl.ColumnImpl;
import net.seesharpsoft.melon.impl.TableImpl;
import net.seesharpsoft.melon.test.TestFixture;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class XmlStorageUT extends TestFixture {
    
    @Override
    public String[] getResourceFiles() {
        return new String[] {
                "/data/Address.xml",
                "/files/Address_New.xml",
                "/files/Address_Test.xml"
        };
    }
    
    @Test
    public void should_parse_address_xml() throws IOException {
        TableImpl table = new TableImpl("Address", new Properties());
        ColumnImpl column = new ColumnImpl(table, "id", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "city", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "country", new Properties());
        table.addColumn(column);
        Properties properties = new Properties();
        properties.put(XmlStorage.PROPERTY_ROOT, "/address/data_record");
        XmlStorage storage = new XmlStorage(table, properties, MelonHelper.getFile("/data/Address.xml"));
        
        List<List<String>> data = storage.read();
        
        assertThat(data.size(), is(2));
    }

    @Test
    public void should_write_address_xml() throws IOException {
        TableImpl table = new TableImpl("Address", new Properties());
        ColumnImpl column = new ColumnImpl(table, "id", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "city", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "country", new Properties());
        table.addColumn(column);
        Properties properties = new Properties();
        properties.put(XmlStorage.PROPERTY_ROOT, "/address/data_record");
        XmlStorage storage = new XmlStorage(table, properties, MelonHelper.getFile("/files/Address_New.xml"));

        List<List<String>> data = storage.read(MelonHelper.getFile("/data/Address.xml"), table, properties);

        storage.write(MelonHelper.getFile("/files/Address_New.xml"), table, properties, data);

        data = storage.read(MelonHelper.getFile("/files/Address_New.xml"), table, properties);
        
        assertThat(data.size(), is(2));
    }

    @Test
    public void should_repeatedly_read_and_write_address_xml_properly() throws IOException {
        TableImpl table = new TableImpl("Address", new Properties());
        ColumnImpl column = new ColumnImpl(table, "id", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "city", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "country", new Properties());
        table.addColumn(column);
        column = new ColumnImpl(table, "userId", new Properties());
        table.addColumn(column);
        Properties properties = new Properties();
        properties.put(XmlStorage.PROPERTY_ROOT, "/address/data_record");
        XmlStorage storage = new XmlStorage(table, properties, MelonHelper.getFile("/files/Address_Test.xml"));

        List<List<String>> data = storage.read(MelonHelper.getFile("/files/Address_Test.xml"), table, properties);
        assertThat(data.size(), is(2));
        
        storage.write(MelonHelper.getFile("/files/Address_Test.xml"), table, properties, data);

        List<List<String>> newData = storage.read(MelonHelper.getFile("/files/Address_Test.xml"), table, properties);

        assertThat(newData.size(), is(2));
        assertThat(newData, is(data));
    }
}
