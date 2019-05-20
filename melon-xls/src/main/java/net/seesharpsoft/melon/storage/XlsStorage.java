package net.seesharpsoft.melon.storage;

import net.seesharpsoft.commons.collection.Properties;
import net.seesharpsoft.melon.Table;
import org.apache.poi.ss.usermodel.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class XlsStorage extends FileStorageBase {

    public static final String PROPERTY_SHEET_NAME = "xls-sheet-name";
    public static final String PROPERTY_SHEET_INDEX = "xls-sheet-index";

    public static final String PROPERTY_FIRST_COLUMN = "xls-first-column";
    public static final String PROPERTY_FIRST_ROW = "xls-first-row";
    public static final String PROPERTY_HEADER = "xls-header";

    public static final String PROPERTY_XSSF = "xls-xssf";

    public XlsStorage(Table table, Properties properties, File file) throws IOException {
        super(table, properties, file);
    }

    protected String getSheetName() {
        return getProperties().getOrDefault(PROPERTY_SHEET_NAME, null);
    }

    protected int getSheetIndex() {
        return getProperties().getOrDefault(PROPERTY_SHEET_INDEX, -1);
    }

    protected int getSheetIndex(Workbook workbook) {
        String sheetName = getSheetName();
        int sheetIndex = getSheetIndex();
        if (sheetName != null) {
            sheetIndex = workbook.getSheetIndex(sheetName);
        }
        if (sheetIndex != -1) {
            return sheetIndex;
        }
        return 0;
    }

    protected boolean getXssf(File file) {
        return getProperties().getOrDefault(PROPERTY_XSSF, file.getName().toLowerCase().endsWith(".xlsx"));
    }

    protected boolean hasHeader() {
        return getProperties().getOrDefault(PROPERTY_HEADER, true);
    }

    protected int getFirstColumn() {
        return getProperties().getOrDefault(PROPERTY_FIRST_COLUMN, 0);
    }

    protected int getFirstRow() {
        return getProperties().getOrDefault(PROPERTY_FIRST_ROW, 0);
    }

    protected Sheet getWorksheet(File file) throws IOException {
        Workbook workbook = null;
        if (file.exists()) {
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                if (fileInputStream.available() == 0) {
                    workbook = WorkbookFactory.create(getXssf(file));
                } else {
                    workbook = WorkbookFactory.create(fileInputStream);
                }
            }
        } else {
            workbook = WorkbookFactory.create(getXssf(file));
        }
        int sheetIndex = getSheetIndex(workbook);
        if (sheetIndex < workbook.getNumberOfSheets()) {
            return workbook.getSheetAt(sheetIndex);
        }
        String sheetName = getSheetName();
        return workbook.createSheet(sheetName != null ? sheetName : file.getName());
    }

    protected List<String> readColumns(Table table, Row row, int firstColumn) {
        int lastColumnIndex = firstColumn + table.getColumns().size();
        List<String> results = new ArrayList<>();
        for (int columnIndex = firstColumn; columnIndex < lastColumnIndex; ++columnIndex) {
            Cell cell = row.getCell(columnIndex, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
            results.add(cell == null ? "" : cell.getStringCellValue());
        }
        return results;
    }

    protected List<List<String>> readRows(Table table, Sheet sheet) {
        int firstRow = getFirstRow() + (hasHeader() ? 1 : 0);
        int lastRowIndex = firstRow + sheet.getLastRowNum();
        int firstColumnIndex = getFirstColumn();
        List<List<String>> results = new ArrayList<>();
        for (int rowIndex = firstRow; rowIndex < lastRowIndex; ++rowIndex) {
            Row row = ensureRow(sheet, rowIndex);
            results.add(readColumns(table, row, firstColumnIndex));
        }
        return results;
    }

    @Override
    protected List<List<String>> read(File file, Table table, Properties properties) throws IOException {
        return readRows(table, getWorksheet(file));
    }

    protected void writeColumns(Table table, Row row, int firstColumn, List<String> columns) {
        int lastColumnIndex = firstColumn + columns.size();
        for (int columnIndex = firstColumn; columnIndex < lastColumnIndex; ++columnIndex) {
            Cell cell = row.getCell(columnIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
            String value = columns.get(columnIndex - firstColumn);
            cell.setCellValue(value);
        }
    }

    protected Row ensureRow(Sheet sheet, int rowIndex) {
        Row row;
        while ((row = sheet.getRow(rowIndex)) == null) {
            row = sheet.createRow(rowIndex);
        }
        return row;
    }

    protected boolean writeHeader(Table table, Sheet sheet, int rowIndex, int firstColumn) {
        if (!hasHeader()) {
            return false;
        }
        Row row = ensureRow(sheet, rowIndex);
        int lastColumnIndex = firstColumn + table.getColumns().size();
        for (int columnIndex = firstColumn; columnIndex < lastColumnIndex; ++columnIndex) {
            Cell cell = row.getCell(columnIndex, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
            cell.setCellValue(table.getColumns().get(columnIndex - firstColumn).getName());
        }
        return true;
    }

    protected void clearRows(Table table, Sheet sheet, int firstRowIndex) {
        int lastRowIndex = firstRowIndex + sheet.getLastRowNum();
        for (int rowIndex = lastRowIndex; rowIndex < lastRowIndex; ++rowIndex) {
            Row row = sheet.getRow(rowIndex);
            if (row != null) {
                sheet.removeRow(row);
            }
        }
    }

    protected void writeRows(Table table, Sheet sheet, List<List<String>> rows) {
        int firstRowIndex = getFirstRow();
        int firstColumnIndex = getFirstColumn();
        if (writeHeader(table, sheet, firstRowIndex, firstColumnIndex)) {
            ++firstRowIndex;
        }
        clearRows(table, sheet, firstRowIndex);
        int lastRowIndex = firstRowIndex + rows.size();
        for (int rowIndex = firstRowIndex; rowIndex < lastRowIndex; ++rowIndex) {
            Row row = ensureRow(sheet, rowIndex);
            List<String> columns = rows.get(rowIndex - firstRowIndex);
            writeColumns(table, row, firstColumnIndex, columns);
        }
    }

    @Override
    protected void write(File file, Table table, Properties properties, List<List<String>> records) throws IOException {
        Sheet sheet = getWorksheet(file);
        Workbook workbook = sheet.getWorkbook();
        String sheetName = getSheetName();
        if (sheetName != null) {
            workbook.setSheetName(workbook.getSheetIndex(sheet), sheetName);
        }
        writeRows(table, sheet, records);
        try (FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            workbook.write(fileOutputStream);
        }
    }
}
