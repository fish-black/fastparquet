package com.fishblack.fastparquet;

import com.fishblack.fastparquet.common.FieldMetadata;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;

public class FieldMetadataTest {

	private static String testinputPath;
	
	private DocumentBuilder builder;

	private Document xmlDoc;
	
	@Before
	public void setUp() throws Exception {
		testinputPath = ConfigUtils.getInstance().getResourceDir() + File.separator + "testdata";
		if (testinputPath == null) {
			Assert.fail("Input resources path is null");
		}

		File importoptions = new File(testinputPath + File.separator + "importoptions.xml");

		if(!importoptions.exists()) {
			Assert.fail("invalid input " + importoptions.getAbsolutePath());
		}
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		builder = factory.newDocumentBuilder();

		xmlDoc = builder.parse(new InputSource(new FileInputStream(importoptions)));
	}

	@Test
	public void testInputTableName() {
		String tableName = FieldMetadata.getInputTableName(xmlDoc);
		Assert.assertNotNull(tableName);
		Assert.assertEquals("Sheet1", tableName);
	}
	
	@Test
	public void testStringType() {
		FieldMetadata fm = new FieldMetadata("f1","integer");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","double");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","number");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","number(38,0)");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","date");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","datetime");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","timestamp");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","");
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1",null);
		Assert.assertFalse(fm.isStringType());
		
		fm = new FieldMetadata("f1","varchar");
		Assert.assertTrue(fm.isStringType());
		
		fm = new FieldMetadata("f1","varchar(12)");
		Assert.assertTrue(fm.isStringType());
	}
}
