package org.z.entities.engine;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Created by Amit on 19/04/2017.
 */
class AvroGenericRecordUtils {

    private final DatumReader<GenericRecord> datumReader;
    private final DatumWriter<GenericRecord> datumWriter;

    public AvroGenericRecordUtils(Schema schema) {
        datumReader = new GenericDatumReader<GenericRecord>(schema);
        datumWriter = new GenericDatumWriter<GenericRecord>(schema);
    }

    public byte[] serialize(GenericRecord record) throws IOException {
        BinaryEncoder encoder = null;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(stream, encoder);

        datumWriter.write(record, encoder);
        encoder.flush();

        return stream.toByteArray();
    }

    public GenericRecord deserialize(byte[] bytes) throws IOException {
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = datumReader.read(null, decoder);
        return record;
    }
}
