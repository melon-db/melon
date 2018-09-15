package net.seesharpsoft.melon.csv;

import net.seesharpsoft.melon.Column;
import net.seesharpsoft.melon.Table;
import net.seesharpsoft.melon.impl.FileStorageBase;
import net.seesharpsoft.melon.sql.SqlHelper;
import org.h2.tools.Csv;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class CsvStorage extends FileStorageBase {

    public static final String IGNORE_HEADER = "ignoreHeader";
    
    public CsvStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    protected boolean ignoreHeader() {
        return Boolean.TRUE.equals(properties.get(IGNORE_HEADER));
    }
    
    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        List<List<String>> records = new ArrayList<>();
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        Csv csv = new Csv();
        try(ResultSet rs  = csv.read(file.getAbsolutePath(), columnNames.toArray(new String[0]), Charset.defaultCharset().name())) {
            records = SqlHelper.fromResultSet(rs);
            if (ignoreHeader() && !records.isEmpty()) {
                records.remove(0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return records;
    }

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        Csv csv = new Csv();
        csv.setWriteColumnHeader(ignoreHeader());
        FileWriter fileWriter = new FileWriter(file);
        try (ResultSet rs = SqlHelper.toResultSet(table, records)) {
            csv.write(fileWriter, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}