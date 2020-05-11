package com.fishblack.fastparquet.utils;

import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.ParquetConversionException;
import com.fishblack.fastparquet.writer.ParquetDataWriter;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ParquetConverter {
	private static final Logger logger = Logger.getLogger(ParquetConverter.class.getName());

	public static long canonicalToParquet(File file, List<FieldMetadata> fields, String parquetPath, Map<String, String> extraMetadata) throws ParquetConversionException, FileNotFoundException  {
		InputStream is = null;
		try {
			is = new FileInputStream(file);
			return canonicalToParquet(is, fields, parquetPath, extraMetadata);
		} finally {
            close(is);
		}
	}

	public static long canonicalToParquet(InputStream in, List<FieldMetadata> fields, String parquetPath, Map<String, String> extraMetadata) throws ParquetConversionException {
		return canonicalToParquet(in, fields,  false, parquetPath, extraMetadata);
	}

	public static long canonicalToParquet(InputStream in, List<FieldMetadata> fields, boolean hasHeader, String parquetPath, Map<String, String> extraMetadata ) throws ParquetConversionException  {
        long rows = 0;
        ParquetDataWriter writer = null;
        CSVReader reader = null;
        try {
            writer = new ParquetDataWriter(fields, parquetPath, extraMetadata);
            reader = new CSVReaderBuilder(new InputStreamReader(in, ParquetAvroUtils.DEFAULT_ENCODING))
                    .withCSVParser(new CSVParserBuilder().withEscapeChar(ParquetAvroUtils.ESCAPE_CHARACTER).build()).build();

			String[] nextLine = reader.readNext();
			if (hasHeader) {
				nextLine = reader.readNext();
			}

			while (nextLine != null) {
				if (nextLine.length == 1) {
					nextLine = reader.readNext();
					continue;
				}
                rows = rows + writer.write(nextLine);
				nextLine = reader.readNext();
			}
		}
		catch (IOException | CsvValidationException ex) {
			logger.log(Level.SEVERE, ex.getMessage(), ex);
			throw new ParquetConversionException("Error occurs when converting to parquet file", ParquetConversionException.ErrorCode.IO_EXCEPTION, ex);
		}
		finally {
            close(reader,writer);
        }
		return rows;
	}

	public static long parquetToCanonical(String parquetPath, OutputStream os, long maxRows) throws IOException {
		return parquetToCanonical(parquetPath, os, maxRows, -1);
	}

	public static long parquetToCanonical(String parquetPath, OutputStream os, long maxRows, int maxColumns) throws IOException {
		long rows = 0;
		ParquetReader<GenericData.Record> reader = null;
		OutputStreamWriter writer = null;
		GenericData model = GenericData.get();
		model.addLogicalTypeConversion(new Conversions.DecimalConversion());
		model.addLogicalTypeConversion(new TimeConversions.DateConversion());//date
		model.addLogicalTypeConversion(new TimeConversions.TimestampConversion());//timestamp

		try {
			AvroParquetReader.Builder<GenericData.Record> builder = AvroParquetReader.<GenericData.Record>builder(new Path(parquetPath));
			reader = builder.withDataModel(model).withConf(new Configuration()).build();
			writer = new OutputStreamWriter(os, ParquetAvroUtils.DEFAULT_ENCODING);
			GenericData.Record record;
			while ((record = reader.read()) != null) {
				writer.write(ParquetAvroUtils.toCsvRecord(record, maxColumns));
				rows++;
				if (maxRows != -1 && rows >= maxRows) {
					break;
				}
			}
		} finally {
			close(reader,writer);
		}
		return rows;
	}

	public static Map<String,String> getMetadata(String parquetPath) throws ParquetConversionException  {
		try {
			return ParquetAvroUtils.getParquetKeyValueMetadata(parquetPath);
		}catch(IllegalArgumentException | IOException ex) {
			logger.log(Level.SEVERE, "Unable to fetch fastparquet metadata from parquet file " + parquetPath, ex);
			throw new ParquetConversionException("Unable to fetch fastparquet metadata from parquet file", ParquetConversionException.ErrorCode.METADATA_FETCH_ERROR);
		}
	}

	static void close(Closeable... closeables) {
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

}
