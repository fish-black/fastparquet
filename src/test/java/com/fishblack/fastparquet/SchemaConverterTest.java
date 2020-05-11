package com.fishblack.fastparquet;

import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.SchemaConverter;
import junit.framework.Assert;
import org.apache.avro.Schema;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;

public class SchemaConverterTest {
	
	private static String testinputDir;
	private static final String fs = File.separator;

	@BeforeClass
	public static void setUp() {
		testinputDir = ConfigUtils.getInstance().getResourceDir() + fs + "testdata" + fs + "parquet";
	}
	
	@Test
	public void testAllTypes() throws ParserConfigurationException, SAXException, IOException {
		Document typeoptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "all_types_non_published_type_options.xml"));
		Schema schema = SchemaConverter.toAvroSchema(FieldMetadata.getPublishedFields(typeoptions));
					
		String actual = schema.toString(true).trim();
		String expected = ParquetTestUtil.fileToStringWithLineEnding(testinputDir + fs + "all_types_non_published_schema_expected.avsc", System.lineSeparator());
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testAllTypesWithPublished() throws ParserConfigurationException, SAXException, IOException {
		Document typeoptions = ParquetTestUtil.readTypeoptionsFromFile(new File(testinputDir + fs + "all_types_excel_published_data_type_options.xml"));
		Schema schema = SchemaConverter.toAvroSchema(FieldMetadata.getPublishedFields(typeoptions));
					
		String actual = schema.toString(true).trim();
		String expected = ParquetTestUtil.fileToStringWithLineEnding(testinputDir + fs + "all_types_excel_published_data_schema_expected.avsc", System.lineSeparator());
			
		Assert.assertEquals(expected, actual);
	}
}
