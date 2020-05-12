package com.fishblack.fastparquet.writer;

import com.fishblack.fastparquet.common.ConvertResult;
import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.ParquetConversionException;
import com.fishblack.fastparquet.common.SchemaConverter;
import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.InvalidSchemaException;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import static com.fishblack.fastparquet.common.ConvertResult.AUDIT_CONVERT_RESULT_KEY;
import static com.fishblack.fastparquet.common.ConvertResult.AUDIT_DETAIL_MESSAGE_KEY;
import static com.fishblack.fastparquet.common.FastParquetConstants.PARQUET_VERSION_KEY;
import static com.fishblack.fastparquet.common.FastParquetConstants.PARQUET_VERSION_VALUE;

/**
 * The parquet file will be automatically created in local disk with the specified parquetPath.
 * Clients should call the write() method continuously until all data are written.
 * Clients should call the close() method when finished writing.
 */
public class ParquetDataWriter implements Closeable {
	private static final Logger logger = Logger.getLogger(ParquetDataWriter.class.getName());

	private final String parquetPath;
	private List<FieldMetadata> fields;
	private Schema sc;
	private ParquetWriter<GenericData.Record> writer;
	private FastAvroWriteSupport<GenericData.Record> writeSupport;
	private Map<String, String> keyValueMetadata;
    private ConvertResult convertResult;
    private long currentRow = 0;
    private List<Field> fieldList ;
    private boolean isAuditMetaDataNeeded;
    
	/**
	 * Constructs a ParquetDataWriter
	 * @param fields List of FieldMetadata.
	 * @param parquetPath The parquet file to write the data into.
	 * @throws IOException 
	 */
	public ParquetDataWriter(List<FieldMetadata> fields, String parquetPath) throws IOException {
		this.fields = fields;
		this.parquetPath = parquetPath;
		this.keyValueMetadata = new HashMap<>();
		this.isAuditMetaDataNeeded = true;
		init();
	}
	
	/**
	 * Constructs a ParquetDataWriter
	 * @param fields List of FieldMetadata.
	 * @param parquetPath The parquet file to write the data into.
	 * @param keyValueMetadata The parquet file level key value metadata.
	 * @throws IOException 
	 */
	public ParquetDataWriter(List<FieldMetadata> fields, String parquetPath, Map<String, String> keyValueMetadata) throws IOException, InvalidSchemaException {
		this.fields = fields;
		this.parquetPath = parquetPath;
		this.keyValueMetadata = keyValueMetadata;
        this.isAuditMetaDataNeeded = true;
		init();
	}

    /**
     * Constructs a ParquetDataWriter
     * @param fields List of FieldMetadata.
     * @param parquetPath The parquet file to write the data into.
     * @param keyValueMetadata The parquet file level key value metadata.
     * @param isAuditMetaDataNeeded Default is true, set it to false means do not write audit metadata into parquet file.
     * @throws IOException
     */
    public ParquetDataWriter(List<FieldMetadata> fields, String parquetPath, Map<String, String> keyValueMetadata, boolean isAuditMetaDataNeeded) throws IOException, InvalidSchemaException {
        this.fields = fields;
        this.parquetPath = parquetPath;
        this.keyValueMetadata = keyValueMetadata;
        this.isAuditMetaDataNeeded = isAuditMetaDataNeeded;
        init();
    }
	
	private void init() throws IOException, InvalidSchemaException {
		sc = SchemaConverter.toAvroSchema(fields);
		fieldList = sc.getFields();
		GenericData model = GenericData.get();
		model.addLogicalTypeConversion(new Conversions.DecimalConversion());
		model.addLogicalTypeConversion(new TimeConversions.DateConversion());//date
		model.addLogicalTypeConversion(new TimeConversions.TimestampConversion());//timestamp

		//Add parquet version into key-value metadata
		if (keyValueMetadata == null){
			keyValueMetadata = new HashMap<>();
		}
		keyValueMetadata.put(PARQUET_VERSION_KEY, PARQUET_VERSION_VALUE);

		Configuration conf = new Configuration();
		
		//turn off writing .crc temp files
		FileSystem fs = FileSystem.get(conf);
		fs.setWriteChecksum(false);

		FastParquetAvroWriter.Builder<GenericData.Record> builder = new FastParquetAvroWriter.Builder<GenericData.Record>(new Path(parquetPath), keyValueMetadata)
																						   .withSchema(sc)
																						   .withConf(conf)
																						   .withCompressionCodec(CompressionCodecName.SNAPPY);
		builder.withDataModel(model);
		
		File parquetFile = new File(parquetPath);
		if(parquetFile.exists()) {
			if (!parquetFile.delete()){
				logger.warning("Parquet file has not been deleted.");
			}
		}
		
		writer = builder.build();
		writeSupport = builder.getWriteSupport();

        convertResult = new ConvertResult();
	}
	
	/**
	 * Write the specified data into parquet file.
	 * The data is a 2 dimensions String array, each record is represented as a String array with field values in canonical format. 
	 * @param data The 2D String array data
	 * @return number of rows actually written
	 * @throws IOException
	 * @throws ParquetConversionException If data length does not match fields length
	 */
	public long write(String[][] data) throws IOException, ParquetConversionException {
	    long rowsWritten = 0;
		String[] record;
		for(int i = 0; i < data.length; i++) {
			record = data[i];
            write(record);
            rowsWritten ++;
		}
		return rowsWritten;
	}
	
	
	/**
	 * Write the specified single row of data into parquet file.
	 * The data is a 1 dimension String array, each record is represented as a String array with field values in canonical format. 
	 * @param data The 1D String array data
	 * @return number of rows actually written
	 * @throws IOException
	 * @throws ParquetConversionException If data length does not match fields length
	 */
	public long write(String[] data) throws IOException, ParquetConversionException {
		
		checkRecordMatchMetadata(data);

		int rowsWritten = 1;
		GenericData.Record avroRecord = new GenericData.Record(sc);
        boolean hasErrorInRow = false;
        Map<String, String> rowFieldErrorMap = new ConcurrentHashMap<>();
		for(int j = 0; j < data.length; j++) {
			Schema fieldSchema = fieldList.get(j).schema();
			//use auto numbered field name to avoid Avro and Parquet invalid field name issue
			String fieldName = "f" + j;

			String valueStr = data[j];
			if(valueStr == null) {
				continue;
			}

			Object avroValue = null;
			try {
				avroValue = ParquetAvroUtils.toAvroFieldValue(fieldSchema, valueStr);
			} catch(UnsupportedOperationException | IllegalArgumentException | ArithmeticException ex) {
                hasErrorInRow = true;
                rowFieldErrorMap.put(fieldName, ex.getMessage() == null ? ex.toString() : ex.getMessage());
			}
			avroRecord.put(fieldName, avroValue);
		}
		
        if (hasErrorInRow){
            convertResult.setFailureCount(convertResult.getFailureCount() + 1);
            convertResult.getErrors().put(currentRow, rowFieldErrorMap);
        }
        else {
            convertResult.setSuccessCount(convertResult.getSuccessCount() + 1);
        }
        writer.write(avroRecord);
        currentRow ++;
        return rowsWritten;
	}

    /**
     * Call once at the end, write audit metadata into parquet file.
     */
	private void writeAuditMetadata() {
        HashMap<String, String> auditMap = new HashMap<>();
        convertResult.setTotalCount(currentRow);
        long failureCount = convertResult.getFailureCount();
        if (failureCount == 0){
            convertResult.setResult(ConvertResult.Result.SUCCESS);
        }
        else if (failureCount > 0 && failureCount < currentRow){
            convertResult.setResult(ConvertResult.Result.PARTIAL_SUCCESS);
        }
        else {
            convertResult.setResult(ConvertResult.Result.FAILED);
        }
        auditMap.put(AUDIT_DETAIL_MESSAGE_KEY, convertResult.toJSON());
        auditMap.put(AUDIT_CONVERT_RESULT_KEY, convertResult.getResult().toString());

        if (keyValueMetadata!=null){
            auditMap.putAll(keyValueMetadata);
        }
        if (writeSupport != null){
            writeSupport.setExtraMetaData(auditMap);
        }
        convertResult = null;
    }

    /**
     * Get current error statistics.
     * @return ConvertResult.InstantStatistic with failedCount and failedPercentage.
     */
    public ConvertResult.InstantStatistic getErrorStatistics(){
        return convertResult.getSnapshot();
    }
	
	/**
	 * Close the underlying parquet writer after writing audit metadata into parquet file.
	 */
	public void close() throws IOException {
	    if (this.isAuditMetaDataNeeded) {
	    	writeAuditMetadata();
        }
		if(writer != null) {
			writer.close();
		}
	}
	
	public List<FieldMetadata> getFields() {
		return this.fields;
	}

	private void checkRecordMatchMetadata(String[] data) throws ParquetConversionException{
		if (data.length != fields.size()){
			throw new ParquetConversionException("Data columns do not match the field metadata columns.", ParquetConversionException.ErrorCode.DATA_LENGTH_ERROR);
		}
	}
	
}
