package com.fishblack.fastparquet;

import com.fishblack.fastparquet.reader.ParquetColumnReader;
import com.fishblack.fastparquet.reader.ParquetColumnReaderImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class ParquetColumnReaderTest {

    private static String testDataDir;
    private static String parquetFile;
    private static String parquetFileWithNulls;
    private static String parquetFileWithChinese;
    private static String parquetFileWithoutData;
    private static final String fs = File.separator;

    @Before
    public void setUp() throws Exception {
        testDataDir = ConfigUtils.getInstance().getResourceDir() + fs + "testdata" + fs + "parquet";
        parquetFile = testDataDir + fs + "all_types_dataset.parquet";
        parquetFileWithNulls = testDataDir + fs + "all_types_dataset_with_nulls.parquet";
        parquetFileWithChinese = testDataDir + fs + "all_types_dataset_with_Chinese.parquet";
        parquetFileWithoutData = testDataDir + fs + "empty_parquet_file.parquet";
    }

    @Test
    public void testReadAllDataTypes() throws IOException {
        // test integer
        List<Object> expected = new ArrayList<>();
        expected.add(133499892);
        expected.add(893499834);
        expected.add(133493498);
        List<Object> actual = readColumnDataToList(parquetFile, "f0");
        Assert.assertEquals(expected, actual);

        // test varchar
        expected = new ArrayList<>();
        expected.add(" Alice");
        expected.add(" Bob");
        expected.add(" John");
        actual = readColumnDataToList(parquetFile, "f1");
        Assert.assertEquals(expected, actual);

        // test decimal
        expected = new ArrayList<>();
        expected.add(new BigDecimal("24.238000000000"));
        expected.add(new BigDecimal("2538293.120000000000"));
        expected.add(new BigDecimal("49.765782000000"));
        actual = readColumnDataToList(parquetFile, "f2");
        Assert.assertEquals(expected, actual);

        // test double
        expected = new ArrayList<>();
        expected.add(new Double("1.24"));
        expected.add(new Double("1.1"));
        expected.add(new Double("1.15"));
        actual = readColumnDataToList(parquetFile, "f4");
        Assert.assertEquals(expected, actual);

        // test Date
        expected = new ArrayList<>();
        expected.add(LocalDate.of(1984, 4, 8));
        expected.add(LocalDate.of(1987, 5, 6));
        expected.add(LocalDate.of(1989, 8, 24));
        actual = readColumnDataToList(parquetFile, "f5");
        Assert.assertEquals(expected, actual);

        // test timestamp
        expected = new ArrayList<>();
        expected.add(LocalDateTime.of(2007, 6, 4, 8, 2, 11));
        expected.add(LocalDateTime.of(2008, 7, 4, 8, 2, 11));
        expected.add(LocalDateTime.of(2010, 8, 12, 11, 2, 11));
        actual = readColumnDataToList(parquetFile, "f6");
        Assert.assertEquals(expected, actual);

        // test time
        expected = new ArrayList<>();
        expected.add(LocalDateTime.of(1970, 1, 1, 8, 2, 11));
        expected.add(LocalDateTime.of(1970, 1, 1, 9, 4, 11));
        expected.add(LocalDateTime.of(1970, 1, 1, 9, 34, 45));
        actual = readColumnDataToList(parquetFile, "f7");
        Assert.assertEquals(expected, actual);
    }

    // only use this for small test files
    public static List<Object> readColumnDataToList(String parquetFile, String columnName) throws IOException {
        try (ParquetColumnReader reader = new ParquetColumnReaderImpl(parquetFile, columnName)) {
            List<Object> values = new ArrayList<>();
            while (reader.hasNext()) {
                values.add(reader.next());
            }
            return values;
        }
    }

    @Test(expected = EOFException.class)
    public void testEndOfFile() throws IOException {
        try (ParquetColumnReader reader = new ParquetColumnReaderImpl(parquetFile, "f0")){
            List<Object> values = new ArrayList<>();
            // test file only has three rows
            for (int i = 0; i < 100; i++) {
                values.add(reader.next());
            }
        }
    }

    @Test
    public void testReadDataWithNulls() throws IOException {
        // test decimal, two nulls
        List<Object> expected = new ArrayList<>();
        expected.add(new BigDecimal("24.238000000000"));
        expected.add(null);
        expected.add(null);
        List<Object> actual = readColumnDataToList(parquetFileWithNulls, "f2");
        Assert.assertEquals(expected, actual);

        // test Date, one null
        expected = new ArrayList<>();
        expected.add(LocalDate.of(1984, 4, 8));
        expected.add(LocalDate.of(1987, 5, 6));
        expected.add(null);
        actual = readColumnDataToList(parquetFileWithNulls, "f5");
        Assert.assertEquals(expected, actual);

        // test timestamp, three nulls
        expected = new ArrayList<>();
        expected.add(null);
        expected.add(null);
        expected.add(null);
        actual = readColumnDataToList(parquetFileWithNulls, "f6");
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testReadDataWithChinese() throws IOException {
        List<Object> expected = new ArrayList<>();
        expected.add(" Alice");
        expected.add("中文测试");
        expected.add("汤姆");
        List<Object> actual = readColumnDataToList(parquetFileWithChinese, "f1");
        Assert.assertEquals(expected, actual);
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testNotExistsColumnName() throws IOException {
        try (ParquetColumnReader reader = new ParquetColumnReaderImpl(parquetFile, "f999999")){
            reader.hasNext();
        } 
    }
    
    @Test
    public void testReadEmptyParquetFile() throws IOException {
        try (ParquetColumnReader reader = new ParquetColumnReaderImpl(parquetFileWithoutData, "id")){
            Assert.assertFalse(reader.hasNext());
        } 
    }

}
