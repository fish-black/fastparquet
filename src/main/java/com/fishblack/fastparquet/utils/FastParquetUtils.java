/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.fishblack.fastparquet.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartDocument;
import javax.xml.stream.events.XMLEvent;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class FastParquetUtils {
	private static final Logger logger = Logger.getLogger(FastParquetUtils.class.getName());
	
    public static final String DATE_FORMAT = "EEE, d MMM yyyy HH:mm:ss z";
    public static final String TIMEZONE_GMT = "GMT";
    
    public static final String UTC_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String TIMEZONE_UTC = "UTC";

	public static final ObjectMapper objectMapper = new ObjectMapper();

	static {
		objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
	}
    
    /**
     * This implementation is bundled with the JRE and we know it works with fastparquet. We've seen other
     * implementations not play nicely with the way fastparquet is coded.
     */
    private static final String PREFERRED_DOCUMENT_BUILDER_FACTORY = "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl";
	
	public static String inputstreamToString(InputStream is) throws IOException
	{
		if (is == null) {
			return null;
		}
		
		return new String(inputstreamToBytes(is), "UTF8");
	}


	public static byte[] inputstreamToBytes(InputStream is) throws IOException
	{
		if (is == null) {
			return null;
		}

		byte[] data;
		ByteArrayOutputStream buf = null;
		try {
			int read;
			buf = new ByteArrayOutputStream();
			byte[] bytes = new byte[4096];
			while ((read = is.read(bytes,0,bytes.length)) != -1) {
				buf.write(bytes, 0, read);
			}

			buf.flush();
			data = buf.toByteArray();
		}finally {
			if (buf != null)
			{
				buf.close();
			}
		}

		return data;
	}
    
    public static Document stringToXMLDocument(String xmlSource)
    {
        Document xmlDoc = null;
        StringReader sr = null;
        try {
            DocumentBuilderFactory factory = getXmlDocBuilderFactory();
            factory.setNamespaceAware(true);
            DocumentBuilder builder = factory.newDocumentBuilder();
            sr = new StringReader(xmlSource);
            xmlDoc = builder.parse(new InputSource(sr));
        } catch (ParserConfigurationException ex) {
            logger.log(Level.INFO, null, ex);
        } catch (SAXException ex) {
            logger.log(Level.INFO, null, ex);
        } catch (IOException ex) {
            logger.log(Level.INFO, null, ex);
        }
        finally {
            if (null != sr) sr.close();
        }
        return xmlDoc;
    }

	public static DocumentBuilderFactory getXmlDocBuilderFactory() {
		DocumentBuilderFactory factory = getPreferredDocumentBuilderFactory();

		try {
			factory.setXIncludeAware(false);
		}
		catch(UnsupportedOperationException ex) {
			//This is not supported on AIX platform..just gobble up the exception and log
			logger.log(Level.FINE, "XML parser does not support setXIncludeAware", ex.getMessage());
		}
		return factory;
	}
 
    public static String urlEncode(String s)
    {
        String returnValue = "";
        try {
            returnValue = URLEncoder.encode(s, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            logger.log(Level.SEVERE, "Failed to URL encode", ex);
            returnValue = s;
        }
        return returnValue;
    }
    
    public static String urlDecode(String s)
    {
        String returnValue = "";
        try {
            returnValue = URLDecoder.decode(s, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            logger.log(Level.SEVERE, "Failed to URL decode", ex);
            returnValue = s;
        }
        return returnValue;
    }
    
    public static String dateToGMTDateString(Date date)
    {
        // Note, DateFormat classes are not thread-safe so create it each time
    	// (ThreadLocal instances possible, but probably overkill)
        SimpleDateFormat dateFormatter = new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(TIMEZONE_GMT));
        String gmtDate = dateFormatter.format(date);
        
        return gmtDate;
    }
    
    public static Date GMTDateStringToDate(String gmtDate) {
    	Date parsedDate = null;
    	
    	try {
    		parsedDate = new SimpleDateFormat(DATE_FORMAT, Locale.ENGLISH).parse(gmtDate);
    	} catch (ParseException e) {
    		logger.log(Level.WARNING, "Cannot parse date {0}", gmtDate);
    	}
    	
    	return parsedDate;
    }
    
    public static String dateToUTCDateString(Date d)
    {
        SimpleDateFormat dateFormatter = new SimpleDateFormat(UTC_DATE_FORMAT);
        dateFormatter.setTimeZone(TimeZone.getTimeZone(TIMEZONE_UTC));
        
        String utcDate =  dateFormatter.format(d);
        return utcDate + "Z"; //jdk 1.6 doesn't support this yet
        
    }
    
    /**
     * Copies as a character stream using default charset
     * Skips first n lines specified by rowstoSkip
     * @param is
     * @param os
     * @param linestoSkip
     * @throws IOException
     */
    public static void copyCharacterStream(InputStream is, OutputStream os, int linestoSkip, int nRows) throws IOException
    {
    	BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"),1024*1024);
    	for(int i = 0; i < linestoSkip; ++i)
    	{
    		br.readLine(); //skip the line
    	}
    	
    	PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(os, "UTF-8"), 1024*1024),false);
    	String line = null;
    	int rowNum = 0;
    	while((line = br.readLine()) != null)
    	{
    		rowNum ++;
    		if(nRows != -1 && rowNum > nRows)
    			break;
    		out.write(line);
    		out.println();
    	}
    	
    	out.flush();	
    }
    
    public static long copyStream(InputStream is, OutputStream os) throws IOException
    {
    	return copyStream(is, os, Long.MAX_VALUE);
    }
    
	/**
	 * Copies a limited number of bytes from an input stream to an output stream.
	 * @param is
	 * @param os
	 * @param maxLen
	 * @return
	 * @throws IOException
	 */
    
    //TODO MaxsizeInputStream already has similar logic that limits the number of bytes 
    //that can be read from an input stream. Consolidate the implementation in one place
    public static long copyStream(InputStream is, OutputStream os, long maxLen) throws IOException {
        byte[] buffer = new byte[1024*1024];
        long totalRead = 0;
        long numLeft = maxLen;

        while (true) {
        	int n = is.read(buffer, 0, (int)Math.min(numLeft, buffer.length));
        	if (n == -1) {
        		break;
        	}
        
        	os.write(buffer, 0, n);
            totalRead += n;

            if (n == numLeft) {
            	break;
            }
            
            numLeft -= n;
        }
        
        return totalRead;
    }

    /**
     * Returns the root cause for an exception.
     * @param t
     * @return
     */
    public static Throwable getRootCause(Throwable t) {
        if (t.getCause() != null && t.getCause() != t) {
            return getRootCause(t.getCause());
        }
        
        return t;
    }

    public static String documentToString(Document doc) throws TransformerException
    {
    	return documentToString(doc, false);
    }

	/**
	 * Converts an XML document object into a string, optionally stripping CRLF
	 * characters.
	 * 
	 * @param doc       an XML document object
	 * @param stripWhitespace whether to strip insignificant whitespace from XML
	 * @return the string representation of the XML document
	 * @throws TransformerException
	 */
    public static String documentToString(Document doc, boolean stripWhitespace) throws TransformerException
    {
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer = tf.newTransformer();

        StringWriter writer = new StringWriter();
        DOMSource domSource = new DOMSource(doc);
		transformer.transform(domSource, new StreamResult(writer));
        String xmlString = writer.getBuffer().toString();

    	return stripWhitespace ? stripWhitespaceFromXML(xmlString) : xmlString;
    }

    public static String nodeToString(Node node) throws TransformerFactoryConfigurationError, TransformerException {
	StringWriter sw = new StringWriter();
	Transformer t = TransformerFactory.newInstance().newTransformer();
	t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "no");
	t.setOutputProperty(OutputKeys.INDENT, "yes");
	t.transform(new DOMSource(node), new StreamResult(sw));
	return sw.toString();
    }
    
    /**
     * Parses date string in ISO8601 format and returns java.util.Date or null
     * if date cannot be parsed.
     * 
     * @param sDate
     *            date string formatted according to ISO8601. Example
     *            "2010-03-26T18:30.00-0400"
     * @return java.util.Date if string is a acceptable date, null otherwise
     */
    public static Date parseDate(String sDate) {
        try {
            // let's try jackson first
            return StdDateFormat.getInstance().parse(sDate);
        } catch (Exception e) {
        }
        try {
            // ISO8601 date java way
            return (new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")).parse(sDate);
        } catch (Exception e) {
        }
        try {
            // it's Unix milliseconds then
            return new Date(Long.parseLong(sDate));
        } catch (Exception e) {
        }
        // cannot parse the thing
        return null;
    }

    public static DocumentBuilderFactory getPreferredDocumentBuilderFactory() {
    	DocumentBuilderFactory factory = null;
    	
    	// Try to load the preferred document builder factory
    	try {
    		factory = DocumentBuilderFactory.newInstance(PREFERRED_DOCUMENT_BUILDER_FACTORY, null);
    	} catch (FactoryConfigurationError e) {
    		logger.log(Level.INFO, "Could not load document builder factory {0}: {1}", new Object[]{PREFERRED_DOCUMENT_BUILDER_FACTORY, e.getMessage()});
    	}
    	
    	// Fall back to whatever's available
    	if (factory == null) {
    		factory = DocumentBuilderFactory.newInstance();
    	}
    	
        return factory;
    }

	public static void close(Closeable... closeables) {
		if (closeables != null) {
			for (Closeable c : closeables) {
				if (c != null) {
					try {
						c.close();
					} catch (IOException e) {
						logger.log(Level.WARNING, "Failed to close closeable", e);
					}
				}
			}
		}
	}
	
	public static String getContentDisposition(String datasetName, String fileType) {
		StringBuffer contentDisposition = new StringBuffer();
		contentDisposition.append("attachment");
		String filename = datasetName;
		CharsetEncoder enc = Charset.forName("US-ASCII").newEncoder();
		boolean canEncode = enc.canEncode(filename);
		if (canEncode) {
		    contentDisposition.append("; filename=").append('"').append(filename).append("." + fileType).append('"');
		} else {
		    enc.onMalformedInput(CodingErrorAction.IGNORE);
		    enc.onUnmappableCharacter(CodingErrorAction.IGNORE);

		    String normalizedFilename = Normalizer.normalize(filename, Form.NFKD);
		    CharBuffer cbuf = CharBuffer.wrap(normalizedFilename);

		    ByteBuffer bbuf;
		    try {
		        bbuf = enc.encode(cbuf);
		    } catch (CharacterCodingException e) {
		        bbuf = ByteBuffer.allocate(0);
		    }

		    String encodedFilename = new String(bbuf.array(), bbuf.position(), bbuf.limit(), Charset.forName("US-ASCII"));

		    if (StringUtils.isNotEmpty(encodedFilename)) {
		        contentDisposition.append("; filename=").append('"').append(encodedFilename).append("." + fileType).append('"');
		    } else {
		    	contentDisposition.append("; filename=").append("\"_Excel").append((new SecureRandom()).nextLong()).append("." + fileType).append('"');
		    }

		    URI uri;
		    try {
		        uri = new URI(null, null, filename, null);
		    } catch (URISyntaxException e) {
		        uri = null;
		    }

		    if (uri != null) {
		    	contentDisposition.append("; filename*=UTF-8''").append(uri.toASCIIString()).append("." + fileType);
		    }
		}
		return contentDisposition.toString();
	}
	
	public static byte[] charsToBytes(char[] srcChars, String charset) {
		CharBuffer charBuffer = CharBuffer.wrap(srcChars);
		ByteBuffer byteBuffer = Charset.forName(charset).encode(charBuffer);
		byte[] srcBytes = Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(), byteBuffer.limit());
		Arrays.fill(byteBuffer.array(), (byte)0);
		return srcBytes;
	}
	
	public static char[] bytesToChars(byte[] srcBytes, String charset) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(srcBytes);
		CharBuffer charBuffer = Charset.forName(charset).decode(byteBuffer);
		char[] srcChars = Arrays.copyOfRange(charBuffer.array(), charBuffer.position(), charBuffer.limit());
		Arrays.fill(charBuffer.array(), (char)0);
		return srcChars;
	}
	
    public static void copyFile(File src, File dest) throws IOException {
	    InputStream in = new FileInputStream(src);
	    try {
	        OutputStream out = new FileOutputStream(dest);
	        try {
	            byte[] buf = new byte[1024*1024];
	            int len;
	            while ((len = in.read(buf)) > 0) {
	                out.write(buf, 0, len);
	            }
	        } finally {
	            out.close();
	        }
	    } finally {
	        in.close();
	    }
   	}
    
    /**
     * Recursively deletes a directory and its contents.
     * @param dir
     * @throws IOException if there is a problem deleting the directory, or any file
     * or subdirectory it contains.
     */
    public static void deleteDirectory(File dir) throws IOException {
        if (!dir.isDirectory()) {
            return;
        }

        File[] files = dir.listFiles();
		if (files == null) {
			throw new IOException(MessageFormat.format("Could not list files in directory {0}", dir));
		}

        for (File file : files) {
            if (file.isFile()) {
				if (!deleteWithRetry(file)) {
					throw new IOException(MessageFormat.format("Could not delete file {0}", file));
				}
            } else {
                deleteDirectory(file);
            }
        }

        if (!deleteWithRetry(dir)) {
            throw new IOException(MessageFormat.format("Could not delete directory {0}", dir));
        }
    }
    
    /**
     * Deletes a file, retrying for a short while if the delete fails. There seems
     * to be a delay sometimes between Java requesting the delete and the operating
     * system executing it.
     * @param file
     * @return
     */
    public static final boolean deleteWithRetry(File file) {
    	if (!file.exists()) {
    		return false;
    	}
    	
    	for (int r=0; r<10; r++) {
    		if (file.delete()) {
    			return true;
    		}
    		
    		try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// NOOP
			}
    	}
    	
    	return !file.exists();
    }
    
    public static void deleteWithWarning(File file) {
    	if (file != null && file.exists() && !file.delete()) {
    		logger.log(Level.WARNING, "Could not delete file {0}", file);
    	}
    }
    
    /**
     * Opens an input stream to a resource on the classpath.
     * @param name
     * @return
     */
    public static InputStream getResourceAsStream(String name) {
    	// One of these techniques should work, both when running as a web app and when running as a standalone
    	// Java application...
    	
    	InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
    	if (is == null) {
    		is = FastParquetUtils.class.getResourceAsStream(name);
    	}
    	return is;
    }
    
    public static String randomAlphaNumeric(int length) {
    	return RandomStringUtils.randomAlphanumeric(length);
    }
    
    public static String getXMLNodeTextContentByTagName(final Document doc, final String tag) {
		String textContent = null;
		NodeList nodes = doc.getElementsByTagName(tag);
		if(nodes.getLength() > 0) {
			textContent = nodes.item(0).getTextContent();
		}
		return textContent;
	}
    
    public static void setXMLNodeTextContentByTagName(final Document doc, final String tag, final String value) {
		NodeList nodes = doc.getElementsByTagName(tag);
		if(nodes.getLength() > 0) {
			nodes.item(0).setTextContent(value);
		}
	}
    
    public static void stripXMLNodeTextContentByTagName(final Document doc, final String tag) {
    	NodeList nodes = doc.getElementsByTagName(tag);
		if(nodes.getLength() > 0 && !nodes.item(0).getTextContent().isEmpty()) {
			nodes.item(0).setTextContent(null);
		}
	}
    
    public static boolean hasXMLNodeByTagName(final Document doc, final String tag) {
		NodeList nodes = doc.getElementsByTagName(tag);
		return nodes.getLength() > 0;
	}

    public static <T extends Enum<?>> T parseEnumInsensitive(Class<T> enumeration,
            String search) {
    	return parseEnumInsensitive(enumeration, search, null);
    }

    public static <T extends Enum<?>> T parseEnumInsensitive(Class<T> enumeration,
            String search, T defaultEnum) {
        for (T each : enumeration.getEnumConstants()) {
            if (search!=null && each.toString().compareToIgnoreCase(search) == 0) {
                return each;
            }
        }
        return defaultEnum;
    }

	public static String stringFromInputStream(InputStream inputStream, String encoding) {
		try {
			ByteArrayOutputStream result = new ByteArrayOutputStream();
			byte[] buffer = new byte[1024 * 1024];
			int length;
			while ((length = inputStream.read(buffer)) != -1) {
				result.write(buffer, 0, length);
			}
			return result.toString(encoding == null ? "UTF-8" : encoding);
		} catch (IOException e) {
			logger.log(Level.WARNING, e.getClass().getName() + ": " + e.getMessage());
		} finally {
			FastParquetUtils.close(inputStream);
		}
		return "";
	}

	public static JsonNode parseJson(String jsonStr) throws IOException {
		return objectMapper.readTree(jsonStr);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> List<T> flattenedArrayToList(String json, Class<T> elementTypeClass) throws JsonParseException, JsonMappingException, IOException {
		if(json == null)
			return null;

		Class arrayTypeClass = Array.newInstance(elementTypeClass, 0).getClass();
		T[] array = (T[])objectMapper.readValue(json, arrayTypeClass);
		return Arrays.asList(array);
	}

	public static String getFormattedString(Document doc) throws TransformerException {
		DOMSource domSource = new DOMSource(doc);
		return transform(domSource);

	}

	private static String transform(DOMSource domSource) throws TransformerFactoryConfigurationError, TransformerConfigurationException, TransformerException {
		StringWriter writer = new StringWriter();
		StreamResult result = new StreamResult(writer);
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.transform(domSource, result);
		return writer.toString();
	}

	public static String getFormattedString(Node newNode) throws TransformerConfigurationException, TransformerFactoryConfigurationError, TransformerException {
		DOMSource domSource = new DOMSource(newNode);
		return transform(domSource);

	}

	public static String applyXslt(Document xmlDoc, InputStream xsltIs) throws TransformerException, UnsupportedEncodingException {
		return applyXslt(new DOMSource(xmlDoc), xsltIs);
	}

	public static String applyXslt(InputStream xmlIs, InputStream xsltIs) throws TransformerException, UnsupportedEncodingException {
		return applyXslt(new StreamSource(xmlIs), xsltIs);
	}

	/**
	 * Applies an XSLT transform to an XML document.
	 * @param source
	 * @param xsltIs input stream to the XSLT
	 * @return the transformed XML as a String
	 * @throws TransformerException
	 */
	public static String applyXslt(Source source, InputStream xsltIs) throws TransformerException, UnsupportedEncodingException {
		TransformerFactory factory = TransformerFactory.newInstance();
		Transformer transformer = factory.newTransformer(new StreamSource(xsltIs));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		StreamResult result = new StreamResult(baos);
		transformer.transform(source, result);
		String transformedXml = baos.toString("UTF-8");
		return transformedXml;
	}


	/******************************************************************************************************************************
	 * BEGIN -- stripWhitespacefromXML related -- BEGIN
	 *******************************************************************************************************************************/
	// Regex patterns to handle xmlDecl
	private static final String XMLSTART = "<\\?xml[ \\t\\r\\n]";
	private static final Pattern XMLSTART_PAT = Pattern.compile(XMLSTART);
	private static final Pattern WHITESPACE_PAT = Pattern.compile("[\\r\\n\\t ]+");

	// "All XML processors must accept the UTF-8 and UTF-16 encodings of Unicode".
	private static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();

	// Factory creation can be costly, factory is not required to be thread-safe and
	// we use a thread-pool, so wrap factories inside a ThreadLocal for safe reuse
	// without incurring synchronization cost
	private static final ThreadLocal<XMLInputFactory> XML_INPUT_FACTORY_FOR_THREAD = new ThreadLocal<XMLInputFactory>() {
		@Override
		public XMLInputFactory initialValue() {
			try {
				return XMLInputFactory.newFactory();
			} catch (javax.xml.stream.FactoryConfigurationError e) {
				logger.log(Level.SEVERE, "Failed to create XMLInputFactory.", e);
			}
			return null;
		}
	};

	public static XMLEventReader createXMLEventReader(InputStream stream, String encoding) throws XMLStreamException {
		XMLInputFactory factory = XML_INPUT_FACTORY_FOR_THREAD.get();
		if (factory == null)
			throw new XMLStreamException("ThreadLocal XMLInputFactory is not initialized.");
		return factory.createXMLEventReader(stream, encoding);
	}

	private static final ThreadLocal<XMLOutputFactory> XML_OUTPUT_FACTORY_FOR_THREAD = new ThreadLocal<XMLOutputFactory>() {
		@Override
		public XMLOutputFactory initialValue() {
			try {
				return XMLOutputFactory.newFactory();
			} catch (javax.xml.stream.FactoryConfigurationError e) {
				logger.log(Level.SEVERE, "Failed to create XMLOutputFactory.", e);
			}
			return null;
		}
	};

	public static XMLEventWriter createXMLEventWriter(OutputStream stream, String encoding) throws XMLStreamException {
		XMLOutputFactory factory = XML_OUTPUT_FACTORY_FOR_THREAD.get();
		if (factory == null)
			throw new XMLStreamException("ThreadLocal XMLOutputFactory is not initialized.");
		return factory.createXMLEventWriter(stream, encoding);
	}

	/**
	 * Removes insignificant whitespaces from a well-formed XML string for storage
	 * and network efficiency.
	 */
	public static String stripWhitespaceFromXML(String originalXML) {
		if (StringUtils.isBlank(originalXML))
			return originalXML;

		// Attempt to parse encoding attribute in XML declaration
		String encoding = DEFAULT_ENCODING;
		try {
			encoding = parseEncodingWithEventReader(originalXML, DEFAULT_ENCODING);
		} catch (UnsupportedEncodingException | XMLStreamException e) {
			logger.log(Level.INFO, "Failed to parse declared encoding from XML using encoding=" + encoding, e);
			return originalXML;
		}

		// Keep a copy of xmlDecl to avoid losing standalone="yes/no"
		String xmlDecl = "";
		if (XMLSTART_PAT.matcher(originalXML).find()) {
			int xmlDeclEndIdx = originalXML.indexOf("?>");
			if (xmlDeclEndIdx > 0) {
				xmlDecl = originalXML.substring(0, xmlDeclEndIdx + 2);
				// Normalize xmlDecl in case it has excess whitespace
				xmlDecl = WHITESPACE_PAT.matcher(xmlDecl).replaceAll(" ");
			}
		}

		XMLEventReader reader = null;
		XMLEventWriter writer = null;
		try {
			ByteArrayInputStream  bais = new ByteArrayInputStream(originalXML.getBytes(encoding));
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			reader = createXMLEventReader(bais, encoding);
			writer = createXMLEventWriter(baos, encoding);
			if (reader == null || writer == null) {
				logger.log(Level.INFO, "Failed to create XML streaming reader/writer pair to stripWhiteSpaceFromXML.");
				return originalXML;
			}

			// Write normalized xmlDecl to output to preserve standalone attribute
			baos.write(xmlDecl.getBytes(encoding));

			XMLEvent previousEvent = null;
			boolean writeEvent = true;
			while (reader.hasNext()) {
				XMLEvent currentEvent = reader.nextEvent();
				writeEvent = true;

				switch (currentEvent.getEventType()) {
					case XMLStreamConstants.CHARACTERS:
						if (currentEvent.asCharacters().isWhiteSpace()) {
							if (previousEvent == null || !previousEvent.isStartElement() || !reader.peek().isEndElement())
								writeEvent = false; // Do NOT write whitespace to output if they are not inside of an element
						}
						break;
					case XMLStreamConstants.START_DOCUMENT:
						writeEvent = false; // Do NOT write xmlDecl to output because XML writer drops "standalone"
						break;
					default:
						break;
				}
				previousEvent = currentEvent;

				if (writeEvent)
					writer.add(currentEvent);
			}

			// Flush and return stripped XML
			writer.flush();
			return baos.toString(encoding);

		} catch (XMLStreamException e) {
			logger.log(Level.INFO, "Failed to stripWhitespaceFromXML.", e);
			return originalXML;
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.INFO, "Failed to convert output into string using encoding=" + encoding, e);
			return originalXML;
		} catch (IOException e) {
			logger.log(Level.INFO, "Failed to write to output stream using encoding=" + encoding, e);
			return originalXML;
		} finally {
			if (writer != null) {
				try {
					writer.flush();
					writer.close();
				} catch (XMLStreamException e) {
					logger.log(Level.INFO, "Error trying to flush and close XMLEventWriter.", e);
				}
			}
			if (reader != null) {
				try {
					reader.close();
				} catch (XMLStreamException e) {
					logger.log(Level.INFO, "Error trying to close XMLEventReader.", e);
				}
			}
		}
	}

	private static String parseEncodingWithEventReader(String originalXML, String defaultEncoding) throws XMLStreamException, UnsupportedEncodingException {
		if (StringUtils.isBlank(originalXML))
			return defaultEncoding;

		XMLEventReader reader = null;
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(originalXML.getBytes(defaultEncoding));
			reader = createXMLEventReader(bais, defaultEncoding);
			if (reader == null)
				return defaultEncoding;

			while (reader.hasNext()) {
				XMLEvent event = reader.nextEvent();
				if (event.isStartDocument()) {
					String parsedEncoding = ((StartDocument) event).getCharacterEncodingScheme();
					return StringUtils.defaultString(parsedEncoding, defaultEncoding);
				}
			}
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (XMLStreamException e) {
					logger.log(Level.INFO, "Error trying to close XMLEventReader.", e);
				}
			}
		}
		return defaultEncoding;
	}
}
