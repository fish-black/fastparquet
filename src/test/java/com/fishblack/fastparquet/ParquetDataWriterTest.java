package com.fishblack.fastparquet;

import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.ParquetConversionException;
import com.fishblack.fastparquet.reader.ParquetDataReader;
import com.fishblack.fastparquet.utils.FastParquetUtils;
import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import com.fishblack.fastparquet.utils.ParquetConverter;
import com.fishblack.fastparquet.writer.ParquetDataWriter;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import junit.framework.Assert;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.ws.rs.ProcessingException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.fishblack.fastparquet.common.FastParquetConstants.PARQUET_VERSION_KEY;
import static com.fishblack.fastparquet.common.FastParquetConstants.PARQUET_VERSION_VALUE;


public class ParquetDataWriterTest {
	
	private static String testinputDir;
	private static final String fs = File.separator;
	private static final String tmpDir = System.getProperty("java.io.tmpdir");
	
	@BeforeClass
	public static void setUp() {
		testinputDir = ConfigUtils.getInstance().getResourceDir() + fs + "testdata" + fs + "parquet";
	}
	
	@Test
	public void testWriteWithPublishedData() throws IOException, ParserConfigurationException, SAXException, ParquetConversionException, CsvValidationException {
		String parquetFilename = UUID.randomUUID().toString() + ".parquet";
		String parquetPath = tmpDir + fs + parquetFilename;
		
		ParquetDataWriter pout = null;
		CSVReader reader = null;
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			//read the content to a new parquet file first
			pout = new ParquetDataWriter(fields, parquetPath);
			CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(testinputDir + fs + "data_types_test.csv"), ParquetAvroUtils.DEFAULT_ENCODING))
					.withCSVParser(new CSVParserBuilder().withEscapeChar(ParquetAvroUtils.ESCAPE_CHARACTER).build());
			reader = csvReaderBuilder.build();
			
			try {
				List<String[]> strValues = new ArrayList<>();
				String[] line = null;
				while((line = reader.readNext()) != null) {
					strValues.add(line);
				}
				pout.write(strValues.toArray(new String[strValues.size()][fields.size()]));
			} finally {
				if(reader != null) {
					reader.close();
				}
				if(pout != null) {
					pout.close();
				}
			}
			
			File outputCsv = File.createTempFile("fastparquet_test", "csv");
			outputCsv.deleteOnExit();
			
			//read the content of parquet file to a csv file
			ParquetConverter.parquetToCanonical(parquetPath, new FileOutputStream(outputCsv), -1);
			
			//compare the content of csv file with the expected content
			String actual = ParquetTestUtil.fileToStringWithLineEnding(outputCsv.getAbsolutePath(), ParquetAvroUtils.LINE_SEPARATOR);
			String expected = ParquetTestUtil.fileToStringWithLineEnding(testinputDir + fs + "data_types_test_expected.csv", ParquetAvroUtils.LINE_SEPARATOR);
			
			Assert.assertEquals(expected, actual);
			
			//test to make sure  temp crc file is not generated
			File crcTempFile = new File(tmpDir + fs + "." + parquetFilename + ".crc");
			Assert.assertFalse(crcTempFile.exists());
		} finally {
			FastParquetUtils.deleteWithWarning(new File(parquetPath));
		}
	}
	
	@Test
	public void testWriteWithKeyValueMetadata() throws IOException, ParserConfigurationException, SAXException, ParquetConversionException, CsvValidationException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		ParquetDataWriter pout = null; 
		CSVReader reader = null;
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			//define the key value metadata to write
			Map<String, String> keyValueMetadata = new HashMap<>();
			keyValueMetadata.put("key1", "value1");
			keyValueMetadata.put("key2", "value2");
			
			//read the content to a new parquet file first
			pout = new ParquetDataWriter(fields, parquetPath, keyValueMetadata);
			CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(testinputDir + fs + "data_types_test.csv"), ParquetAvroUtils.DEFAULT_ENCODING))
					.withCSVParser(new CSVParserBuilder().withEscapeChar(ParquetAvroUtils.ESCAPE_CHARACTER).build());
			reader = csvReaderBuilder.build();
			
			try {
				List<String[]> strValues = new ArrayList<>();
				String[] line = null;
				while((line = reader.readNext()) != null) {
					strValues.add(line);
				}
				pout.write(strValues.toArray(new String[strValues.size()][fields.size()]));
			} finally {
				if(reader != null) {
					reader.close();
				}
				if(pout != null) {
					pout.close();
				}
			}
			
			ParquetDataReader pin = null;
			try {
				pin = new ParquetDataReader(parquetPath);
				
				Map<String, String> actualKeyValueMetadata = pin.getKeyValueMetadata();
				Assert.assertEquals(keyValueMetadata.get("key1"), actualKeyValueMetadata.get("key1"));
				Assert.assertEquals(keyValueMetadata.get("key2"), actualKeyValueMetadata.get("key2"));
			} finally {
				if(pin != null) {
					pin.close();
				}
			}
		} finally {
			FastParquetUtils.deleteWithWarning(new File(parquetPath));
		}
	}
	

	@Test
	public void testWriteNumberDT() throws IOException, ParquetConversionException {
		File tmpFile = File.createTempFile("tmp", ".parquet");
		tmpFile.delete();
		tmpFile.deleteOnExit();
		String[] data1 = {"2110173.5099999998"};
		String[] data2 = {"2293517.4240000001"};
		
		List<FieldMetadata> fields = new ArrayList<FieldMetadata>(); 
		fields.add(new FieldMetadata("FIELD1", "number"));
		ParquetDataWriter writer = new ParquetDataWriter(fields, tmpFile.getAbsolutePath());
	
		try {
			writer.write(data1);
			writer.write(data2);
		}finally {
			writer.close();
			
		}
	
		Assert.assertTrue(tmpFile.exists());
		ParquetDataReader reader = new ParquetDataReader(tmpFile.getAbsolutePath());
		
		try {
			String[][] rows = reader.next(2);
			Assert.assertEquals(2, rows.length);
			Assert.assertEquals(data1[0], rows[0][0]);
			Assert.assertEquals(data2[0], rows[1][0]);
		} finally {
			reader.close();
		}
	}
	
	@Test
	public void testWriteDoubleDT() throws IOException, ParquetConversionException {
		File tmpFile = File.createTempFile("tmp", ".parquet");
		tmpFile.delete();
		tmpFile.deleteOnExit();
		String[] data1 = {"2110173.5099999998"};
		String[] data2 = {"2293517.4240000001"};
		
		List<FieldMetadata> fields = new ArrayList<FieldMetadata>(); 
		fields.add(new FieldMetadata("FIELD1", "double"));
		ParquetDataWriter writer = new ParquetDataWriter(fields, tmpFile.getAbsolutePath());
	
		try {
			writer.write(data1);
			writer.write(data2);
		}finally {
			writer.close();
			
		}
	
		Assert.assertTrue(tmpFile.exists());
		ParquetDataReader reader = new ParquetDataReader(tmpFile.getAbsolutePath());
		
		try {
			String[][] rows = reader.next(2);
			Assert.assertEquals(2, rows.length);
			Assert.assertEquals(Double.parseDouble(data1[0]), new Double(rows[0][0]));
			Assert.assertEquals(Double.parseDouble(data2[0]), new Double(rows[1][0]));
		} finally {
			reader.close();
		}
	}
	
	@Test
	public void testWriteTimestamp() throws Exception {
		File tmpFile = File.createTempFile("tmp", ".parquet");
		tmpFile.delete();
		tmpFile.deleteOnExit();
		String[] data1 = { "88525", "1330999859", "P1000001", "TL26119", "General Electric",
				"GE 25-Foot Phone Line Cord", "7.99", "30878261197", "2000-01-04T00:00:00.000Z","2011-04-01T00:00:00.000Z","1975-01-01T04:23:55.345Z" };

		List<FieldMetadata> fields = new ArrayList<FieldMetadata>();
		fields.add(new FieldMetadata("f0", "number"));
		fields.add(new FieldMetadata("f1", "number"));
		fields.add(new FieldMetadata("f2", "varchar(8)"));
		fields.add(new FieldMetadata("f3", "varchar(18)"));
		fields.add(new FieldMetadata("f4", "varchar(29)"));
		fields.add(new FieldMetadata("f5", "varchar(113)"));
		fields.add(new FieldMetadata("f6", "number"));
		fields.add(new FieldMetadata("f7", "number"));
		fields.add(new FieldMetadata("f8", "timestamp"));
		fields.add(new FieldMetadata("f9", "date"));
		fields.add(new FieldMetadata("f10", "time"));

		ParquetDataWriter writer = new ParquetDataWriter(fields, tmpFile.getAbsolutePath());

		try {
			writer.write(data1);
		} finally {
			writer.close();
		}

		Assert.assertTrue(tmpFile.exists());
		ParquetDataReader reader = new ParquetDataReader(tmpFile.getAbsolutePath());

		try {
			String[][] rows = reader.next(1);
			Assert.assertEquals(1, rows.length);
			Assert.assertEquals(11, rows[0].length);
			Assert.assertEquals("2000-01-04 00:00:00.0", rows[0][8]);
			Assert.assertEquals("2011-04-01", rows[0][9]);
			Assert.assertEquals("04:23:55.345", rows[0][10]);
		} finally {
			reader.close();
		}
	}
	
	@Test(expected = ParquetConversionException.class)
	/**
	 * A ParquetConversionException should be thrown in this case, for the data length does not match the field metadata.
	 */
	public void testOutofSyncMetadata() throws IOException, ParquetConversionException {
		List<FieldMetadata> fields = new LinkedList<FieldMetadata>();
		fields.add(new FieldMetadata("id","number"));
		fields.add(new FieldMetadata("salary_prep","number"));
		fields.add(new FieldMetadata("gender","varchar(10)"));
		
		File tmpFile = File.createTempFile("tmp", ".parquet");
		tmpFile.delete();
		tmpFile.deleteOnExit();
		String[] data1 = {"1","male" };
		ParquetDataWriter writer = new ParquetDataWriter(fields, tmpFile.getAbsolutePath());
		try {
			writer.write(data1);
		}
		finally {
			writer.close();
		}
	}
	
	@Test
	public void testParquetMetadataWithVersion() throws IOException, ParquetConversionException {
		List<FieldMetadata> fields = new LinkedList<FieldMetadata>();
		fields.add(new FieldMetadata("id","number"));
		fields.add(new FieldMetadata("gender","varchar(10)"));
		
		File tmpFile = File.createTempFile("tmp", ".parquet");
		tmpFile.delete();
		tmpFile.deleteOnExit();
		String[] data1 = {"1","male" };
		ParquetDataWriter writer = new ParquetDataWriter(fields, tmpFile.getAbsolutePath());
		try {
			writer.write(data1);
		}
		finally {
			writer.close();
		}
		
		Assert.assertTrue(tmpFile.exists());
		ParquetDataReader reader = new ParquetDataReader(tmpFile.getAbsolutePath());

		try {
			Map<String, String> extraMetadata = reader.getKeyValueMetadata();
            Assert.assertEquals(PARQUET_VERSION_VALUE, extraMetadata.get(PARQUET_VERSION_KEY));
		} finally {
			reader.close();
		}
	}

	@Test
	public void testCSVEmptyStringDataToParquet() throws ParquetConversionException, IOException, ParserConfigurationException, SAXException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "empty_string_data_type_options.xml"));
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			Map<String, String> extraMetadata = new HashMap<>();

			ParquetConverter.canonicalToParquet(new FileInputStream(testinputDir + fs + "empty_string_data.csv"), fields, false,  parquetPath, extraMetadata);

			Object[][] records = readParquetDataValue(parquetPath);
			Assert.assertNotNull(records);
			Assert.assertNotNull(records[0]);

			Assert.assertEquals("  ", records[0][1].toString());
			Assert.assertNull(records[1][1]);
			Assert.assertNull(records[2][1]);
			Assert.assertEquals("  ", records[3][1].toString());

		} finally {
			FastParquetUtils.deleteWithWarning(new File(parquetPath));
		}
	}

	private Object[][] readParquetDataValue(String parquetFilePath) throws IOException{
		ParquetReader<GenericData.Record> reader = null;
		try {
			GenericData model = GenericData.get();
			model.addLogicalTypeConversion(new Conversions.DecimalConversion());
			model.addLogicalTypeConversion(new TimeConversions.DateConversion());//date
			model.addLogicalTypeConversion(new TimeConversions.TimestampConversion());//timestamp
			AvroParquetReader.Builder<GenericData.Record> builder = AvroParquetReader.<GenericData.Record>builder(new Path(parquetFilePath));
			reader = builder.withDataModel(model).withConf(new Configuration()).build();
			List<Object[]> rowsRead = new ArrayList<>();
			GenericData.Record record;
			int numOfFields = 0;
			while ((record = reader.read()) != null) {
				if (numOfFields == 0) {
					numOfFields = record.getSchema().getFields().size();
				}
				Schema sc = record.getSchema();
				List<Schema.Field> fields = sc.getFields();
				Object[] objValues = new Object[fields.size()];
				for (int i = 0; i < fields.size(); i++) {
					Object fieldValue = record.get(i);
					objValues[i] = fieldValue;
				}
				rowsRead.add(objValues);
			}
			return rowsRead.toArray(new Object[rowsRead.size()][numOfFields]);
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
}
