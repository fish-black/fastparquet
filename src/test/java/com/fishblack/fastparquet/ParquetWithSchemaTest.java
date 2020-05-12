package com.fishblack.fastparquet;

import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.ParquetConversionException;
import com.fishblack.fastparquet.utils.Utils;
import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import com.fishblack.fastparquet.utils.ParquetConverter;
import junit.framework.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ParquetWithSchemaTest {
	
	private static String testinputDir;
	private static final String fs = File.separator;
	private static final String tmpDir = System.getProperty("java.io.tmpdir");

	@BeforeClass
	public static void setUp() {
		testinputDir = ConfigUtils.getInstance().getResourceDir() + fs + "testdata" + fs + "parquet";
	}
	
	@Test
	public void testAllTypesConversion() throws IOException, ParquetConversionException, ParserConfigurationException, SAXException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "all_types_data_no_transform_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);

			ParquetConverter.canonicalToParquet(new FileInputStream(testinputDir + fs + "all_types_data_no_transform.csv"), fields, true,  parquetPath, null);

			File outputCsv = File.createTempFile("fastparquet_test", "csv");
			outputCsv.deleteOnExit();
			
			ParquetConverter.parquetToCanonical(parquetPath, new FileOutputStream(outputCsv), -1);
			
			String actual = ParquetTestUtil.fileToStringWithLineEnding(outputCsv.getAbsolutePath(), ParquetAvroUtils.LINE_SEPARATOR);
			String expected = ParquetTestUtil.fileToStringWithLineEnding(testinputDir + fs + "all_types_data_no_transform_expected.csv", ParquetAvroUtils.LINE_SEPARATOR);
			
			Assert.assertEquals(expected, actual);
		} finally {
			Utils.deleteWithWarning(new File(parquetPath));
		}
	}
	
	@Test
	public void testAllTypesConversionWithPublished() throws ParquetConversionException, IOException, ParserConfigurationException, SAXException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "all_types_excel_published_data_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			ParquetConverter.canonicalToParquet(new FileInputStream(testinputDir + fs + "all_types_excel_published_data.csv"), fields, false,  parquetPath, null);
			
			File outputCsv = File.createTempFile("fastparquet_test", "csv");
			outputCsv.deleteOnExit();
			
			ParquetConverter.parquetToCanonical(parquetPath, new FileOutputStream(outputCsv), -1);
			
			String actual = ParquetTestUtil.fileToStringWithLineEnding(outputCsv.getAbsolutePath(), ParquetAvroUtils.LINE_SEPARATOR);
			String expected = ParquetTestUtil.fileToStringWithLineEnding(testinputDir + fs + "all_types_excel_published_data_expected.csv", ParquetAvroUtils.LINE_SEPARATOR);
			
			Assert.assertEquals(expected, actual);
		} finally {
			Utils.deleteWithWarning(new File(parquetPath));
		}
	}
	
	@Test
	public void testDataWithAllTypes() throws ParquetConversionException, IOException, ParserConfigurationException, SAXException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			ParquetConverter.canonicalToParquet(new FileInputStream(testinputDir + fs + "data_types_test.csv"), fields, false,  parquetPath, null);
			
			File outputCsv = File.createTempFile("fastparquet_test", "csv");
			outputCsv.deleteOnExit();
			
			ParquetConverter.parquetToCanonical(parquetPath, new FileOutputStream(outputCsv), -1);
			
			String actual = ParquetTestUtil.fileToStringWithLineEnding(outputCsv.getAbsolutePath(), ParquetAvroUtils.LINE_SEPARATOR);
			String expected = ParquetTestUtil.fileToStringWithLineEnding(testinputDir + fs + "data_types_test_expected.csv", ParquetAvroUtils.LINE_SEPARATOR);
			
			Assert.assertEquals(expected, actual);
		} finally {
			Utils.deleteWithWarning(new File(parquetPath));
		}
	}
	
	@Test
	public void testParquetKeyValueMetadata() throws ParquetConversionException, IOException, ParserConfigurationException, SAXException {
		String parquetPath = tmpDir + fs + UUID.randomUUID().toString() + ".parquet";
		try {
			Document typeOptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "data_types_test_type_options.xml"));
			
			List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeOptions);
			
			Map<String, String> extraMetadata = new HashMap<>();
			extraMetadata.put("sheetName", "sheet A");
			extraMetadata.put("otherKey", "value1");
			
			ParquetConverter.canonicalToParquet(new FileInputStream(testinputDir + fs + "data_types_test.csv"), fields, false,  parquetPath, extraMetadata);
			
			Map<String, String> expectedMetadata = ParquetAvroUtils.getParquetKeyValueMetadata(parquetPath);
			
			Assert.assertEquals("sheet A", expectedMetadata.get("sheetName"));
			Assert.assertEquals("value1", expectedMetadata.get("otherKey"));
			
		} finally {
			Utils.deleteWithWarning(new File(parquetPath));
		}
	}

}
