package com.fishblack.fastparquet.writer;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

public class FastAvroWriteSupport<T> extends org.apache.parquet.avro.AvroWriteSupport<T> {

	Map<String, String> extraMetaData;

	public FastAvroWriteSupport(MessageType schema, Schema avroSchema, GenericData model,
	                            Map<String, String> extraMetadata) {
		super(schema, avroSchema, model);
		this.extraMetaData = extraMetadata;
	}

	@Override
	public WriteContext init(Configuration configuration) {

		WriteContext context = super.init(configuration);
	
		Map<String, String> metadata = new HashMap<>();
		metadata.putAll(context.getExtraMetaData());
		if(extraMetaData != null) {
			metadata.putAll(extraMetaData);
		}
		
		WriteContext newContext = new WriteContext(context.getSchema(), metadata);
		return newContext;
	}

	@Override
    public FinalizedWriteContext finalizeWrite() {
        return new FinalizedWriteContext(this.extraMetaData == null ? new HashMap<>() : this.extraMetaData);
    }

    public void setExtraMetaData(Map<String, String> extraMetaData) {
        this.extraMetaData = extraMetaData;
    }
}