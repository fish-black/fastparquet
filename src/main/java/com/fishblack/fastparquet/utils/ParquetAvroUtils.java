package com.fishblack.fastparquet.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fishblack.fastparquet.common.ConvertResult;
import com.fishblack.fastparquet.common.UnsupportedDataTypeException;
import com.fishblack.fastparquet.reader.ParquetDataReader;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class ParquetAvroUtils {
	public static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String TIMESTAMP_FORMAT_FOR_PUBLISHED = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public static final DateTimeFormatter dateFormatter = DateTimeFormat.forPattern(DATE_FORMAT).withZoneUTC();
	public static final DateTimeFormatter timestampFormatter = DateTimeFormat.forPattern(TIMESTAMP_FORMAT).withZoneUTC();
	public static final DateTimeFormatter timestampFormatterPublished = DateTimeFormat.forPattern(TIMESTAMP_FORMAT_FOR_PUBLISHED).withZoneUTC();
	public static final String LINE_SEPARATOR = "\n";
	public static final String DEFAULT_ENCODING = "utf-8";
	public static final char ESCAPE_CHARACTER = (char)0x0;
	public static final char COMMA_CHARACTER = ',';
	public static final String EMPTY_STRING = "";
	public static final Pattern POINT_ZERO_PATTERN = Pattern.compile("\\.0$");
	private static final String[] SUPPORTED_COLUMN_DATA_TYPE = {"INT32", "UTF8", "DOUBLE", "DECIMAL", "TIMESTAMP_MILLIS", "DATE", "TIME"};
	
	//used for formatting, remove the scientific notation
	public static final NumberFormat numberFormatter = new DecimalFormat();
	
	//used for parsing double, handles both with or without comma
	public static final NumberFormat numberParser = new DecimalFormat();
	
	static {
		numberFormatter.setGroupingUsed(false);
		numberFormatter.setMaximumFractionDigits(15);
		
		numberParser.setGroupingUsed(true);
	}

	public static String[] toStringArray(GenericData.Record record) {
		Schema sc = record.getSchema();
		List<Field> fields = sc.getFields();
		String[] strValues = new String[fields.size()];
		for (int i = 0; i < fields.size(); i++) {
			Schema fieldSchema = fields.get(i).schema();
			Object fieldValue = record.get(i);
			strValues[i] = ParquetAvroUtils.toCanonicalString(fieldSchema, fieldValue);
		}
		return strValues;
	}

	public static String toCsvRecord(GenericData.Record record) {
		return toCsvRecord(record, -1);
	}

	public static String toCsvRecord(GenericData.Record record, int maxColumn) {
		Schema sc = record.getSchema();
		maxColumn = maxColumn < 0 ? sc.getFields().size() : maxColumn;
		int fieldsCounter = Math.min(sc.getFields().size(), maxColumn);
		StringBuilder sb = new StringBuilder();

		List<Field> fields = sc.getFields();
		for (int i = 0; i < fieldsCounter; i++) {
			Schema fieldSchema = fields.get(i).schema();
			Object fieldValue = record.get(i);
			sb.append(ParquetAvroUtils.toCanonicalString(fieldSchema, fieldValue) + ",");
		}
		if (sb.length() > 0) {
			sb.deleteCharAt(sb.length() - 1);
		}
		sb.append(LINE_SEPARATOR);
		return sb.toString();
	}

	public static String toCanonicalString(Schema avroFieldSchema, Object fieldValue) {
		Schema fieldSchema = avroFieldSchema;
		Schema.Type type = fieldSchema.getType();
		LogicalType logicType = fieldSchema.getLogicalType();
		if(fieldValue == null) {
			return EMPTY_STRING;
		}
		
		//optional field is handled as union in Avro, such as ["null", "string"], we need to find out the type which is not null
		if(type == Schema.Type.UNION) {
			List<Schema> types = fieldSchema.getTypes();
			for(Schema s : types) {
			    Schema.Type innerType = s.getType();
			    if(innerType  != Schema.Type.NULL) {
			    	type = innerType;
			    	logicType = s.getLogicalType();
			    	fieldSchema = s;
			    	break;
			    }
			}
		}
		
		if(logicType != null) {
			String val = EMPTY_STRING;
			switch (logicType.getName()) {
			case "date":
				LocalDate date = (LocalDate)fieldValue;
				val = ParquetAvroUtils.dateFormatter.print(date);
				break;
			case "timestamp-millis":
				DateTime timestamp = (DateTime)fieldValue;
				val = ParquetAvroUtils.timestampFormatter.print(timestamp);
				
				//Spark doesn't support time, we treat time as timestamp and use fixed date "1970-01-01"	
				//now we need to remove the date part when converting to canonical
				String actualType = fieldSchema.getProp("actualType");
				if("time".equals(actualType)) {
					//remove the date part if the actual type is time
					val = val.substring(11) ;
					val = val.replace(".000", EMPTY_STRING);
				} else {
					// for timestamp remove meaningless 000 milliseconds, we use ".0" because a lot of fastparquet and BiServer tests uses it.
					val = val.replace(".000", ".0");
				}
				
				break;
			case "decimal":
				//stripe extra zeros when default scale is used
				String isDefaultScale = fieldSchema.getProp("isDefaultScale");
				if(isDefaultScale != null && Boolean.parseBoolean(isDefaultScale)) {
					BigDecimal dec = (BigDecimal)fieldValue;
					val = dec.stripTrailingZeros().toPlainString();
				} else {
					val = ((BigDecimal)fieldValue).toPlainString();
				}
				break;
			default:
				val = fieldValue.toString();
			}
			return val;
		} else {
			String val;
			if(type.equals(Schema.Type.DOUBLE)) {
				Double dval = (Double)fieldValue;
				val = numberFormatter.format(dval);
				
				//a workaround to remove the trailing ".0" when the actual data is an integer or long but data type is double in type-options
				val = POINT_ZERO_PATTERN.matcher(val).replaceAll(EMPTY_STRING);
			} else if (type.equals(Schema.Type.STRING)){
				val = StringEscapeUtils.escapeCsv(fieldValue.toString());
			} else {
				val = fieldValue.toString();
			}
			return val;
		}
	}

	//convert field value in canonical format into Avro Strong typed field value
	public static Object toAvroFieldValue(Schema fieldSchema, String srcValue) {
		Schema.Type type = fieldSchema.getType();
		LogicalType logicType = fieldSchema.getLogicalType();
		Object result = srcValue;
		String trimmedVal = srcValue.trim();
		
		
		//optional field is handled as union in Avro, such as ["null", "string"], we need to find out the actual type which is not null in the union
		
		if(type == Schema.Type.UNION) {
			List<Schema> types = fieldSchema.getTypes();
			for(Schema s : types) {
			    Schema.Type innerType = s.getType();
			    if(innerType  != Schema.Type.NULL) {
			    	type = innerType;
			    	logicType = s.getLogicalType();
			    	fieldSchema = s;
			    	break;
			    }
			}
		}
		
		if(trimmedVal.equals(EMPTY_STRING)) {
		    if((logicType != null) || (!type.equals(Schema.Type.STRING))) {
		        return null;
		    }
		}
		
		if(logicType == null) {
			//it is primitive type
			switch (type) {
			case INT:
				try {
					result = Integer.parseInt(trimmedVal);
				}
				catch(NumberFormatException ex) {
					throw new IllegalArgumentException("Input data "+ trimmedVal +" is not a valid integer data");
				}
				break;
			case LONG:
				try {
					result =  Long.parseLong(trimmedVal);
				}
				catch(NumberFormatException ex) {
					throw new IllegalArgumentException("Input data "+ trimmedVal +" is not a valid long data");
				}
				break;
			case DOUBLE:
				//remove the group character "," before parsing
				trimmedVal = removeComma(trimmedVal);
				try {
					result = Double.parseDouble(trimmedVal);
				}
				catch(NumberFormatException ex) {
					throw new IllegalArgumentException("Input data "+ trimmedVal +" is not a valid double data");
				}
				break;
			case STRING:
				result = StringUtils.isEmpty(srcValue) ? null : srcValue;
				break;
			default:
				break;
			}
		} else {
			//it is logic type, handle field value per Avro Spec
			switch (logicType.getName()) {
			case "decimal":
				//remove the group character "," before parsing
				trimmedVal = removeComma(trimmedVal);
				
				LogicalTypes.Decimal dt = (LogicalTypes.Decimal)logicType;
				BigDecimal dec;
				try {
					dec = new BigDecimal(trimmedVal);
				}
				catch(NumberFormatException ex) {
					throw new IllegalArgumentException("Input data "+ trimmedVal +" is not a valid decimal");
				}
				
				Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
				int scale = dt.getScale();
				dec = dec.setScale(scale, BigDecimal.ROUND_DOWN);
	
				 result  = conversion.toFixed(dec, fieldSchema, logicType);
				break;
				
			case "date":
				try {
					trimmedVal = removeStartEndSingleQuotation(trimmedVal);
					result = ParquetAvroUtils.dateFormatter.parseLocalDate(trimmedVal);
				} catch(UnsupportedOperationException | IllegalArgumentException ex) {
					//if parsing with date format failed, try it again with published timetamp format, this is a workaround for the format issue in published data
					DateTime datetime;
					try{
						datetime = ParquetAvroUtils.timestampFormatterPublished.parseDateTime(trimmedVal);
					}
					catch (IllegalArgumentException e){
						throw new IllegalArgumentException("Input data "+ trimmedVal +" is not in date format");
					}
					result = datetime.toLocalDate();
				}
				break;
				
			case "timestamp-millis":	
				String actualType = fieldSchema.getProp("actualType");
				String val = removeStartEndSingleQuotation(trimmedVal);
				
				//if it doesn't have fractional seconds, set it to ".000" to avoid parsing error
				if(!val.contains(".")) {
					val = val + ".000";
				}
				
				//check the custom property actualType to see if it is a time
				if("time".equals(actualType)) {
					//spark doesn't support time, we treat time as timestamp and use fixed date "1970-01-01"
					//in addition, published data from data prep ends with 'Z' and already has date in it, we skip adding "1970-01-01" for it.
					if(!val.endsWith("Z")) {
						val = "1970-01-01 " + val;
					}
				}
				
				try {
					result = ParquetAvroUtils.timestampFormatter.parseDateTime(val);
				} catch(UnsupportedOperationException | IllegalArgumentException ex) {
					//if parsing with timestamp format failed, try it again with published timetamp format, this is a workaround for the format issue in published data
					try {
						result = ParquetAvroUtils.timestampFormatterPublished.parseDateTime(val);
					}
					catch (IllegalArgumentException e){
						throw new IllegalArgumentException("Input data "+ trimmedVal +" is not in timestamp format");
					}
				}
				break;
			}
		}
		return result;
	}
	
	public static Map<String, String> getParquetKeyValueMetadata(String parquetPath) throws IllegalArgumentException, IOException {
		ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(parquetPath));
		return reader.getFileMetaData().getKeyValueMetaData();
	}
	
    public static List<Type> getParquetFileColumns(String parquetFile) throws IOException {
        try(ParquetFileReader reader = ParquetFileReader.open(new Configuration(), new Path(parquetFile))) {
            MessageType schema = reader.getFileMetaData().getSchema();
            return schema.getFields();
        }
    }

	public static String removeComma(String resource){
		if (!StringUtils.isEmpty(resource) && resource.indexOf(COMMA_CHARACTER) >= 0) {
			StringBuilder builder = new StringBuilder();
			int position = 0;
			char currentChar;

			while (position < resource.length()) {
				currentChar = resource.charAt(position++);
				if (currentChar != COMMA_CHARACTER) builder.append(currentChar);
			}
			return builder.toString();
		}
		else return resource;
	}

	public static String removeStartEndSingleQuotation(String dateTimeStr){
		if (StringUtils.isEmpty(dateTimeStr)){
			return dateTimeStr;
		}
		while(dateTimeStr.charAt(0) == '\''){
			dateTimeStr = dateTimeStr.substring(1);
		}
		while(dateTimeStr.charAt(dateTimeStr.length()-1) == '\''){
			dateTimeStr = dateTimeStr.substring(0, dateTimeStr.length()-1);
		}
		return dateTimeStr;
	}

	public static boolean isSupportedParquetFile(String parquetPath) throws IOException{
		try {
			Map<String, String> metadata = getParquetKeyValueMetadata(parquetPath);
			if (metadata.get(ConvertResult.AUDIT_CONVERT_RESULT_KEY) == null){
				for (Type type : getParquetFileColumns(parquetPath)) {
					if (!isSupportedParquetDataType(type)){
						return false;
					}
				}
			}
			else if (metadata.get(ConvertResult.AUDIT_DETAIL_MESSAGE_KEY) != null){
				ConvertResult auditInfo = new ObjectMapper().readValue(metadata.get(ConvertResult.AUDIT_DETAIL_MESSAGE_KEY), ConvertResult.class);
				long rowNumber = auditInfo.getTotalCount();
				if (rowNumber > 0) {
					ParquetDataReader reader = null;
					try {
						reader = new ParquetDataReader(parquetPath);
						String[][] records = reader.next(1);
						if (records.length != 1) {
							return false;
						}
					}
					finally {
						if (reader != null) {
							reader.close();
						}
					}
				}
			}
		}
		catch (RuntimeException ex){
			return false;
		}
		return true;
	}

	public static boolean isSupportedParquetDataType(Type columnType){
		String orgType = getOrgTypeFromSchema(columnType);
		return Arrays.asList(SUPPORTED_COLUMN_DATA_TYPE).contains(orgType);
	}

	public static String getOrgTypeFromSchema(Type columnType){
		String orgType = "";
		if (columnType.getOriginalType() != null) {
			orgType = columnType.getOriginalType().name();
		} else {
			orgType = columnType.asPrimitiveType().getPrimitiveTypeName().name();
		}
		return orgType;
	}

	public static String getDataTypeFromSchema(Type columnType) throws UnsupportedDataTypeException {
		String fieldType = "unknown";
		String orgType = getOrgTypeFromSchema(columnType);
		switch (orgType) {
			case "INT32":
				fieldType = "integer";
				break;
			case "DOUBLE":
				fieldType = "double";
				break;
			case "DATE":
				fieldType = "date";
				break;
			case "TIME":
				fieldType = "time";
				break;
			case "TIMESTAMP_MILLIS":
				fieldType = "timestamp";
				break;
			case "DECIMAL":
				fieldType = "number";
				break;
			case "UTF8":
				fieldType = "varchar";
				break;
			default:
				throw new UnsupportedDataTypeException(String.format("%s is not supported in parquet dataset.", orgType));
		}
		return fieldType;
	}

}
