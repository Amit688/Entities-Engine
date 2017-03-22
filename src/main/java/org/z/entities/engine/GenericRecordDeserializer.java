package org.z.entities.engine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

public class GenericRecordDeserializer implements Deserializer<GenericRecord> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public GenericRecord deserialize(String topic, byte[] data) {
        try{
        	Schema schema = new Schema.Parser().parse(new ByteArrayInputStream(data));
        	DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
            return datumReader.read(null, DecoderFactory.get().binaryDecoder(data, null));
        } catch(IOException exception){
            exception.printStackTrace();
            return null;
        }
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}
