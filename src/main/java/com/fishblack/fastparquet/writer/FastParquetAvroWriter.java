package com.fishblack.fastparquet.writer;

import com.fishblack.fastparquet.writer.FastAvroWriteSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

import java.io.IOException;
import java.util.Map;

public class FastParquetAvroWriter<T> extends org.apache.parquet.avro.AvroParquetWriter<T> {

	@Deprecated
	public FastParquetAvroWriter(Path file, Schema avroSchema) throws IOException {
		super(file, avroSchema);
	}

	public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
		private Schema schema = null;
		private GenericData model = SpecificData.get();
		private Map<String, String> extraMetadata ;
        private FastAvroWriteSupport<T> writeSupport;

		public Builder(Path file, Map<String, String> metadata) {
			super(file);
			this.extraMetadata = metadata;
		}

		public Builder<T> withSchema(Schema schema) {
			this.schema = schema;
			return this;
		}

		public Builder<T> withDataModel(GenericData model) {
			this.model = model;
			return this;
		}

		protected Builder<T> self() {
			return this;
		}

        protected WriteSupport<T> getWriteSupport(Configuration conf) {
		    writeSupport = new FastAvroWriteSupport<T>(new AvroSchemaConverter(conf).convert(schema), schema, model, extraMetadata);
            return writeSupport;
		}

        protected FastAvroWriteSupport<T> getWriteSupport(){
            return this.writeSupport;
        }

	}

}
