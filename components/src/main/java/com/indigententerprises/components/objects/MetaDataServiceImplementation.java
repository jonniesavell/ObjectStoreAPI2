package com.indigententerprises.components.objects;

import com.indigententerprises.services.objects.MetaDataService;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jonniesavell
 */
public class MetaDataServiceImplementation implements MetaDataService {

    private static final String ATTRIBUTE_NAME = "attributeName";
    private static final String ATTRIBUTE_TYPE = "attributeType";
    private static final String ATTRIBUTE_VALUE = "attributeValue";

    private static final Schema RECORD_SCHEMA;

    static {
        RECORD_SCHEMA =
                SchemaBuilder.record("Attribute")
                             .fields()
                             .name(ATTRIBUTE_NAME).type().stringType()
                                                         .noDefault()
                             .name(ATTRIBUTE_TYPE).type().unionOf()
                                                         .nullType()
                                                         .and()
                                                         .stringType()
                                                         .endUnion()
                                                         .nullDefault()
                             .name(ATTRIBUTE_VALUE).type().unionOf()
                                                          .nullType()
                                                          .and()
                                                          .booleanType()
                                                          .and()
                                                          .doubleType()
                                                          .and()
                                                          .floatType()
                                                          .and()
                                                          .intType()
                                                          .and()
                                                          .longType()
                                                          .and()
                                                          .stringType()
                                                          .endUnion()
                                                          .nullDefault()
                             .endRecord();
    }

    /**
     * outputStream must be non-null and must correspond to a live stream
     *   that was truncated prior to invocation
     */
    @Override
    public void serializeMetaData(
            final OutputStream outputStream,
            final Map<String, Object> attributes) throws IOException {

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(RECORD_SCHEMA);
        final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

        try {
            dataFileWriter.create(RECORD_SCHEMA, outputStream);

            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                final String type =
                        entry.getValue() == null
                        ? "null"
                        : entry.getValue().getClass().getName();
                final GenericRecordBuilder recordBuilder =
                        new GenericRecordBuilder(RECORD_SCHEMA)
                                .set(ATTRIBUTE_NAME, entry.getKey())
                                .set(ATTRIBUTE_TYPE, type)
                                .set(ATTRIBUTE_VALUE, entry.getValue());
                final GenericRecord attributeRecord = recordBuilder.build();
                dataFileWriter.append(attributeRecord);
            }
        } finally {
            dataFileWriter.close();
        }
    }

    /**
     * inputStream must be non-null and must correspond to a live stream
     *   populated with data that this service recognizes
     */
    @Override
    public Map<String, Object> deserializeMetaData(final InputStream inputStream) throws IOException {

        final Map<String, Object> result = new HashMap<>();
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        final DataFileStream<GenericRecord> dataReader = new DataFileStream<>(inputStream, datumReader);

        try {
            // this is how you read the schema out of the file
            final Schema schema = dataReader.getSchema();

            // this is how you retrieveObjectMetaData the fields from the schema
            final Collection<Schema.Field> schemaFields = schema.getFields();

            for (final Schema.Field schemaField : schemaFields) {
                // TODO: this is handy during the learning phase but not helpful in production.
                System.out.println("name        : " + schemaField.name());
                System.out.println("schema-type : " + schemaField.schema().getType());
                System.out.println("position    : " + schemaField.pos());
            }

            while (dataReader.hasNext()) {

                final GenericRecord attributeData = dataReader.next();
                final Object value = attributeData.get(ATTRIBUTE_VALUE);
                final Object finalValue;

                if (value instanceof Utf8) {
                    Utf8 realValue = (Utf8) value;
                    finalValue = realValue.toString();
                } else {
                    finalValue = value;
                }

                final Utf8 utf8 = (Utf8) attributeData.get(ATTRIBUTE_NAME);
                result.put(utf8.toString(), finalValue);
            }

            return result;
        } finally {
            dataReader.close();
        }
    }
}
