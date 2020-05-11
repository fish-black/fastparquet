package com.fishblack.fastparquet.common;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.List;

/**
 * Field Metadata for a field which comes from the dataset type-options-xml. 
 * It includes the field name and field type, both are extracted from type-options-xml.
 */
public class FieldMetadata {
	
	private String fieldName;
	
	private String fieldType;

	public FieldMetadata(String fieldName, String fieldType) {
		this.fieldName = fieldName;
		this.fieldType = fieldType;
	}

	/**
	 * Returns the field name 
	 * @return field name
	 */
	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}

	/**
	 * Returns the field type
	 * @return field type
	 */
	public String getFieldType() {
		return fieldType;
	}

	public void setFieldType(String fieldType) {
		this.fieldType = fieldType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
		result = prime * result + ((fieldType == null) ? 0 : fieldType.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FieldMetadata other = (FieldMetadata) obj;
		if (fieldName == null) {
			if (other.fieldName != null)
				return false;
		} else if (!fieldName.equals(other.fieldName))
			return false;
		if (fieldType == null) {
			if (other.fieldType != null)
				return false;
		} else if (!fieldType.equals(other.fieldType))
			return false;
		return true;
	}
	
	public static List<FieldMetadata> getInputFields(Document doc) {
		return getInputFields(doc, null);
	}
	
	/**
	 * This method should only work with input section
	 * 
	 */
	public static List<FieldMetadata> getInputFields(Document doc, String tableName) {
		List<FieldMetadata> fields = new ArrayList<>();

		Element tables = (Element)doc.getElementsByTagName("ds:tables").item(0);
		NodeList inputTables = tables.getElementsByTagName("ds:table"); 
		
		Element inputTable = null;
		if (inputTables != null) {
			if(tableName == null) {
				inputTable = (Element) inputTables.item(0);
			} else {
				for(int i=0; i < inputTables.getLength(); i++) {
					Element table = (Element) inputTables.item(i);
					String name = table.getAttribute("name");
					if(name.equals(tableName)) {
						inputTable = table;
						break;
					}
				}
			}
			
			if(inputTable == null) {
				inputTable = (Element) inputTables.item(0);
			}

			NodeList columns = inputTable.getElementsByTagName("ds:column");
			for (int i = 0; i < columns.getLength(); i++) {
				Element column = (Element) columns.item(i);

				NodeList children = column.getChildNodes();
				String fieldName = "";
				for(int j = 0; j < children.getLength(); j++) {
					Node child = children.item(j);
					if("ds:name".equals(child.getNodeName())) {
						fieldName = child.getTextContent();
						break;
					}
				}

				NodeList fieldTypes = column.getElementsByTagName("ds:datatype");
				String fieldType = ((Element) fieldTypes.item(0)).getTextContent();

				fields.add(new FieldMetadata(fieldName, fieldType));
			}
		}
		return fields;
	}

	public static List<FieldMetadata> getPublishedFields(Document doc) {
		List<FieldMetadata> inputFields = getInputFields(doc);

		List<FieldMetadata> fields = new ArrayList<>();

		NodeList prepareTables = doc.getElementsByTagName("ds:prepareTables");
		Element prepareTable = null;
		if (prepareTables != null) {
			prepareTable = (Element) prepareTables.item(0);
			if(prepareTable != null) {
				Element columns = (Element)prepareTable.getElementsByTagName("ds:columns").item(0);
				if(columns != null) {
					NodeList preColumns = columns.getElementsByTagName("ds:column");
					for (int i = 0; i < preColumns.getLength(); i++) {
						Element column = (Element) preColumns.item(i);

						NodeList colChildren = column.getChildNodes();
						String fieldName = "";
						for(int j = 0; j < colChildren.getLength(); j++) {
							Node child = colChildren.item(j);
							if(child.getNodeName() == "ds:name") {
								fieldName = child.getTextContent();
								break;
							}
						}

						String transformType = column.getAttribute("transform");

						//ignore the fields from BiServer transform
						if(transformType != null && "bis".equals(transformType)) {
							continue;
						}

						NodeList fieldTypes = column.getElementsByTagName("ds:datatype");
						String fieldType = null;
						if(fieldTypes.getLength() != 0) {
							fieldType = ((Element) fieldTypes.item(0)).getTextContent();
						} else {
							//find type from input table 
							for(FieldMetadata m : inputFields) {
								if(m.getFieldName().equals(fieldName)){
									fieldType = m.getFieldType();
									break;
								}
							}
						}

						fields.add(new FieldMetadata(fieldName, fieldType));
					}
				}
			}
		}

		if(fields.isEmpty()) {
			fields.addAll(inputFields); //takes care of previous version XML
		}
		return fields;
	}

	public static String getInputTableName(Document doc) {
		NodeList inputTables = doc.getElementsByTagName("ds:tables");
		
		String tableName = null;
		if (inputTables != null) {
			Element tables = (Element) inputTables.item(0);
			Element table = (Element)tables.getElementsByTagName("ds:table").item(0);
			tableName = table.getAttribute("name");
		}
		
		return tableName;
	}

	public boolean isStringType() {
		return this.fieldType != null && this.fieldType.startsWith("varchar");
	}
}
