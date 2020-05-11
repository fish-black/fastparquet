package com.fishblack.fastparquet;

import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.ParquetConversionException;
import com.fishblack.fastparquet.reader.ParquetDataReader;
import com.fishblack.fastparquet.utils.FastParquetUtils;
import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import com.fishblack.fastparquet.writer.ParquetDataWriter;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ParquetDataReaderTest {

	private static String testinputDir;
	private static final String fs = File.separator;
	private static final String tmpDir = System.getProperty("java.io.tmpdir");
	
	@BeforeClass
	public static void setUp() {
		testinputDir = ConfigUtils.getInstance().getResourceDir() + fs + "testdata" + fs + "parquet";
	}
	
	@Test
	public void testReadWithPublishedData() throws IOException, ParserConfigurationException, SAXException, ParquetConversionException, CsvValidationException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		ParquetDataWriter pout = null;
		ParquetDataReader pin = null;
		CSVReader reader = null;
		try {
			Document typeOptions =  ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			//create the new parquet file first
			pout = new ParquetDataWriter(fields, parquetPath);
			CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(testinputDir + fs + "data_types_test.csv"), ParquetAvroUtils.DEFAULT_ENCODING))
					.withCSVParser(new CSVParserBuilder().withEscapeChar(ParquetAvroUtils.ESCAPE_CHARACTER).build());
			reader = csvReaderBuilder.build();
			
			try {
				String[] line = null;
				String[][] data = null;
				while((line = reader.readNext()) != null) {
					data = new String[1][fields.size()];
					data[0] = line;
					pout.write(data);
				}
			} finally {
				if(reader != null) {
					reader.close();
				}
				if(pout != null) {
					pout.close();
				}
			}
			
			//get the content of the new parquet file by ParquetInputStream
			StringBuilder builder = new StringBuilder();
			try {
				pin = new ParquetDataReader(parquetPath);
				String[][] records = pin.next(1);
				builder.append(toCSVString(records));
				
				records = pin.next(1);
				builder.append(toCSVString(records));
				
				records = pin.next(5);
				builder.append(toCSVString(records));
				
				records = pin.next(100);
				builder.append(toCSVString(records));
			} finally {
				if (pin != null)
					pin.close();
			}
			
			//compare the contents read from the new parquet file with expected result
			// remove one extra line separator from toCSVString method
			builder.setLength(builder.length() - ParquetAvroUtils.LINE_SEPARATOR.length());
			String actual = builder.toString();
			String expected = ParquetTestUtil.fileToStringWithLineEnding(testinputDir + fs + "data_types_test_expected.csv", ParquetAvroUtils.LINE_SEPARATOR);
			
			Assert.assertEquals(expected, actual);
			
		} finally {
			FastParquetUtils.deleteWithWarning(new File(parquetPath));
		}
	}
	
	private String toCSVString(String[][] records) {
		StringBuilder builder = new StringBuilder();
		for(String[] record : records) {
			for(String s : record) {
				builder.append(s + ",");
			}
			if (builder.length() > 0) {
				builder.deleteCharAt(builder.length() - 1);
			}
			builder.append(ParquetAvroUtils.LINE_SEPARATOR);
		}
		return builder.toString();
	}
	
	@Test
	public void testReadEmptyParquetFile() throws IOException, ParserConfigurationException, SAXException, ParquetConversionException{
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		ParquetDataWriter pout = null; 
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			pout = new ParquetDataWriter(fields, parquetPath);
			
			//write a parquet file with zero records
			try {
				pout.write(new String[0][fields.size()]);
			} finally {
				if(pout != null) {
					pout.close();
				}
			}
			
			Assert.assertTrue(new File(parquetPath).exists());
			
			ParquetDataReader pin = null;
			try {
				pin = new ParquetDataReader(parquetPath);
				String[][] records = pin.next(10);
				
				Assert.assertEquals(records.length, 0);
			} finally {
				if( pin != null ) {
					pin.close();
				}
			}
		} finally {
			FastParquetUtils.deleteWithWarning(new File(parquetPath));
		}
	}
	
	@Test
	public void testReadKeyValueMetadata() throws IOException, ParserConfigurationException, SAXException, ParquetConversionException, CsvValidationException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		ParquetDataWriter pout = null; 
		ParquetDataReader pin = null;
		CSVReader reader = null;
		try {
			Document typeOptions =  ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			//define the key value metadata to write
			Map<String, String> keyValueMetadata = new HashMap<>();
			keyValueMetadata.put("key1", "value1");
			keyValueMetadata.put("key2", "value2");
			
			//create the new parquet file first
			pout = new ParquetDataWriter(fields, parquetPath, keyValueMetadata);
			CSVReaderBuilder csvReaderBuilder = new CSVReaderBuilder(new InputStreamReader(new FileInputStream(testinputDir + fs + "data_types_test.csv"), ParquetAvroUtils.DEFAULT_ENCODING))
					.withCSVParser(new CSVParserBuilder().withEscapeChar(ParquetAvroUtils.ESCAPE_CHARACTER).build());
			reader = csvReaderBuilder.build();
			
			try {
				String[] line = null;
				String[][] data = null;
				while((line = reader.readNext()) != null) {
					data = new String[1][fields.size()];
					data[0] = line;
					pout.write(data);
				}
			} finally {
				if(reader != null) {
					reader.close();
				}
				if(pout != null) {
					pout.close();
				}
			}
			
			try {
				pin = new ParquetDataReader(parquetPath);
				pin.next(1);
				
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

}
