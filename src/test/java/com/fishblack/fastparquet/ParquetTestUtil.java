package com.fishblack.fastparquet;

import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class ParquetTestUtil {
	
	public static Document readTypeoptionsFromFile(File typeoptionsFile) throws ParserConfigurationException, SAXException, IOException
	{
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setNamespaceAware(true);
		DocumentBuilder builder = factory.newDocumentBuilder();
		
		return builder.parse(typeoptionsFile);
		
		
	}

	public static String fileToStringWithLineEnding(String path, String lineSeparator) throws IOException {
		StringBuilder sb = new StringBuilder();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(path), ParquetAvroUtils.DEFAULT_ENCODING));
			String line = br.readLine();
			while (line != null) {
				sb.append(line).append(lineSeparator);
				line = br.readLine();
			}
		} finally {
			if (br!=null) {
				br.close();
			}
		}
		return sb.toString().trim();
	}

}
