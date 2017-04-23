package org.z.entities.engine;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;

/**
 * Created by Amit on 19/04/2017.
 */
class AvroGenericRecordUtils {

    static byte[] encode(GenericRecord record, Schema schema) throws IOException {

        GenericDatumWriter<GenericRecord>
                datumWriter =
                new GenericDatumWriter<>(schema);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byteArrayOutputStream.reset();
        BinaryEncoder binaryEncoder = new EncoderFactory().binaryEncoder(byteArrayOutputStream, null);
        datumWriter.write(record, binaryEncoder);
        binaryEncoder.flush();
        byte[] bytes = byteArrayOutputStream.toByteArray();
        return bytes;
    }


    static GenericRecord decode(byte[] recordBytes, Schema schema) throws IOException {
        GenericDatumReader<GenericRecord>
                datumReader =
                new GenericDatumReader<>(schema);
        ByteArrayInputStream stream = new ByteArrayInputStream(recordBytes);
//        stream.reset();
        BinaryDecoder binaryDecoder = new DecoderFactory().binaryDecoder(stream, null);
        GenericRecord record = null;
        while (true) {
            try {
                record = datumReader.read(null, binaryDecoder);
            } catch (EOFException e) {
                break;
            }
        }
        return record;
    }
}
