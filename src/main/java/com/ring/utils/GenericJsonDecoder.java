package com.ring.utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.Schema;

public class GenericJsonDecoder {

    private Schema schema;
    private DatumReader<GenericRecord> reader;

    public GenericJsonDecoder(Schema schema) {
        this.schema = schema;
        this.reader = new GenericDatumReader<>(this.schema);
    }

    public GenericRecord decode(String json) throws Exception {      
        try {
            JsonDecoder decoder = new DecoderFactory().jsonDecoder(this.schema, json);
            return this.reader.read(null, decoder);
        } catch (Exception exception) {
            throw exception;
        }
        
    }

}
