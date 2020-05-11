package com.fishblack.fastparquet.reader;

import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The parquet file needs to be in a local path which is readable by fastparquet first.
 * The clients can call next() to retrieve the amount of data they need.
 * The clients should call close() if read is finished.
 */
public class ParquetDataReader implements Closeable{
	
	private final String parquetPath;
	private ParquetReader<GenericData.Record> reader = null;
	private Map<String, String> keyValueMetadata = null;

	/**
	 * Constructs a ParquetDataReader
	 * @param parquetPath The parquet file to read data from.
	 * @throws IOException 
	 */
	public ParquetDataReader(String parquetPath) throws IOException {
		this.parquetPath = parquetPath;
		init();
	}
	
	private void init() throws IOException {
		GenericData model = GenericData.get();
		model.addLogicalTypeConversion(new Conversions.DecimalConversion());
		model.addLogicalTypeConversion(new TimeConversions.DateConversion());//date
		model.addLogicalTypeConversion(new TimeConversions.TimestampConversion());//timestamp
		AvroParquetReader.Builder<GenericData.Record> builder = AvroParquetReader.<GenericData.Record>builder(new Path(parquetPath));
		reader = builder.withDataModel(model).withConf(new Configuration()).build();
	}
	
	/**
	 * Read next rows of data. The data is a 2D String array, each row is represented as a String array with each fields' value in canonical String.
	 * The order of fields is same as the fields order when used for writing.
	 * The size of the outer array is the actual number of rows read, it might be less than the parameter rows if less data is available.
	 * @param rows The max number of rows to read
	 * @return The data read by this operation
	 * @throws IOException
	 */
	public String[][] next(int rows) throws IOException {
		List<String[]> rowsRead = new ArrayList<>();
		int numRows = 0;
		String[] currentRow;
		GenericData.Record record;
		int numOfFields = 0;
		while ((record = reader.read()) != null) {
			if(numOfFields == 0) {
				numOfFields = record.getSchema().getFields().size();
			}
			currentRow = ParquetAvroUtils.toStringArray(record);
			rowsRead.add(currentRow);
			numRows++;
			if (numRows >= rows) {
				break;
			}
		}
		return rowsRead.toArray(new String[rowsRead.size()][numOfFields]);
	}
	
	/**
	 * Close the underlying parquet reader.
	 */
	public void close() throws IOException {
		reader.close();
	}
	
	/**
	 * Get the key value metadata of the parquet file associated with this reader.
	 * @return the map of key value metadata
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public Map<String, String> getKeyValueMetadata() throws IllegalArgumentException, IOException {
		if(keyValueMetadata == null) {
			keyValueMetadata = ParquetAvroUtils.getParquetKeyValueMetadata(parquetPath);
		}
		return keyValueMetadata;
	}
}
