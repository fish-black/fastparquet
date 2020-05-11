package com.fishblack.fastparquet.common;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SchemaConverter {

	private static final Logger logger = Logger.getLogger(SchemaConverter.class.getName());
	private static int DEFAULT_PRECISION = 38;
	private static int MAX_PRECISION = 38;
	private static int DEFAULT_SCALE = 12;
	private static int DEFAULT_FIXED_SIZE = 16; //16 is the size required in parquet to store a decimal with precision 38.
	
	public static Schema toAvroSchema(List<FieldMetadata> fields) {
		logger.fine(":" + fields.toString());
		FieldAssembler<Schema> schema = SchemaBuilder.record("GenericRecord").namespace("fastparquet").fields();
		
		int index = 0;
		for (FieldMetadata field : fields) {
			// we use auto generated field name to avoid AVRO and Parquet  invalid field name issue
			String fieldName = "f" + index;
			index++;
			
			String fieldType = field.getFieldType();
			
			Schema fieldSchema = oacTypeToAvroSchema(fieldType, fieldName);
			
			schema.name(fieldName).type().optional().type(fieldSchema);
		}
		
		Schema sc = schema.endRecord();
		
		logger.fine("Avro schema:" + sc.toString(true));
		
		return sc;
	}

	private static Schema oacTypeToAvroSchema(String oacType, String fieldName) {
		Schema schema = Schema.create(Schema.Type.INT);
		if (oacType.startsWith("varchar") || oacType.startsWith("geometry")) {
			schema = Schema.create(Schema.Type.STRING);
		} else if (oacType.equals("double")) {
			schema = Schema.create(Schema.Type.DOUBLE);
		} else if (oacType.equals("date")) {
			schema = Schema.create(Schema.Type.INT);
			schema = LogicalTypes.date().addToSchema(schema);
		} else if (oacType.equals("time")) {
			schema = Schema.create(Schema.Type.LONG);
			schema = LogicalTypes.timestampMillis().addToSchema(schema);
			schema.addProp("actualType", "time");
		} else if (oacType.equals("timestamp")) {
			schema = Schema.create(Schema.Type.LONG);
			schema = LogicalTypes.timestampMillis().addToSchema(schema);
		} else if (oacType.startsWith("number")) {
			String pattern = "number\\(?(?<precision>\\d*)\\,?\\s*(?<scale>\\d*)\\)?";
			Pattern p = Pattern.compile(pattern);
			Matcher m = p.matcher(oacType);
			String precision = null, scale = null;
			int precisionInt = 0;
			int scaleInt = 0;
			if (m.matches()) {
				precision = m.group("precision");
				scale = m.group("scale");
			}
			LogicalTypes.Decimal dec = null;
			
			//TODO, use BYTEs here will not work when read the parquet file in Spark, needs more investigation
			//schema = Schema.create(Schema.Type.BYTES);
			//Use fixed size for now
			schema = Schema.createFixed(fieldName + "_type", "fastparquet", "", DEFAULT_FIXED_SIZE);
			
			if(StringUtils.isNotBlank(precision)) {
				precisionInt = Integer.parseInt(precision);
			} else {
				precisionInt = DEFAULT_PRECISION;
				schema.addProp("isDefaultPrecision", "true");
			}
			
			if(StringUtils.isNotBlank(scale)) {
				scaleInt = Integer.parseInt(scale);
			} else {
				scaleInt = DEFAULT_SCALE;
				schema.addProp("isDefaultScale", "true");
			}
			
			if(precisionInt > MAX_PRECISION) {
				//if precision exceeds MAX_PRECISION, we reset both precision and scale here
				precisionInt = MAX_PRECISION;
				scaleInt = DEFAULT_SCALE;
			}
			
			//scale needs to less or equal to precision
			scaleInt = Math.min(precisionInt, scaleInt);
			
			dec = LogicalTypes.decimal(precisionInt, scaleInt); 
			schema = dec.addToSchema(schema);
		}
		return schema;
	}
}
