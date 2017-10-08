/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.converter;

import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.*;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.DateFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.DecimalFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.TimeFieldConverter;
import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical.TimestampFieldConverter;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

//looks like Avro and JSON + Schema is convertible by means of
//a unified conversion approach since they are using the
//same the Struct/Type information ...
public class AvroJsonSchemafulRecordConverter implements RecordConverter {

    public static final Set<String> LOGICAL_TYPE_NAMES = new HashSet<>(
            Arrays.asList(Date.LOGICAL_NAME, Decimal.LOGICAL_NAME,
                            Time.LOGICAL_NAME, Timestamp.LOGICAL_NAME)
        );

    private final Map<Schema.Type, SinkFieldConverter> converters = new HashMap<>();
    private final Map<String, SinkFieldConverter> logicalConverters = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(AvroJsonSchemafulRecordConverter.class);

    public AvroJsonSchemafulRecordConverter() {

        //standard types
        registerSinkFieldConverter(new BooleanFieldConverter());
        registerSinkFieldConverter(new Int8FieldConverter());
        registerSinkFieldConverter(new Int16FieldConverter());
        registerSinkFieldConverter(new Int32FieldConverter());
        registerSinkFieldConverter(new Int64FieldConverter());
        registerSinkFieldConverter(new Float32FieldConverter());
        registerSinkFieldConverter(new Float64FieldConverter());
        registerSinkFieldConverter(new StringFieldConverter());
        registerSinkFieldConverter(new BytesFieldConverter());

        //logical types
        registerSinkFieldLogicalConverter(new DateFieldConverter());
        registerSinkFieldLogicalConverter(new TimeFieldConverter());
        registerSinkFieldLogicalConverter(new TimestampFieldConverter());
        registerSinkFieldLogicalConverter(new DecimalFieldConverter());
    }

    @Override
    public BsonDocument convert(Schema schema, Object value) {

        if(schema == null || value == null) {
            throw new DataException("error: schema and/or value was null for AVRO conversion");
        }

        return toBsonDoc(schema, value);

    }

    private void registerSinkFieldConverter(SinkFieldConverter converter) {
        converters.put(converter.getSchema().type(), converter);
    }

    private void registerSinkFieldLogicalConverter(SinkFieldConverter converter) {
        logicalConverters.put(converter.getSchema().name(), converter);
    }

    private BsonDocument toBsonDoc(Schema schema, Object value) {
        BsonDocument doc = new BsonDocument();
        schema.fields().forEach(f -> processField(doc, (Struct)value, f));
        return doc;
    }

    private void processField(BsonDocument doc, Struct struct, Field field) {

        logger.trace("processing field '{}'",field.name());

        if(isSupportedLogicalType(field.schema())) {
            doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field),field.schema()));
            return;
        }

        try {
            switch(field.schema().type()) {
                case BOOLEAN:
                case FLOAT32:
                case FLOAT64:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case STRING:
                case BYTES:
                    handlePrimitiveField(doc, struct, field);
                    break;
                case STRUCT:
                    handleStructField(doc, struct, field);
                    break;
                case ARRAY:
                    handleArrayField(doc, struct, field);
                    break;
                case MAP:
                    handleMapField(doc, struct, field);
                    break;
                default:
                    throw new DataException("unexpected / unsupported schema type " + field.schema().type());
            }
        } catch (Exception exc) {
            throw new DataException("error while processing field " + field.name(), exc);
        }

    }

    private void handleMapField(BsonDocument doc, Struct struct, Field field) {
        logger.trace("handling complex type 'map'");
        BsonDocument bd = new BsonDocument();
        if(struct.get(field)==null) {
            logger.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }
        Map m = (Map)struct.get(field);
        for(Object entry : m.keySet()) {
            String key = (String)entry;
            if(field.schema().valueSchema().type().isPrimitive()) {
                bd.put(key, getConverter(field.schema().valueSchema()).toBson(m.get(key),field.schema()));
            } else {
                bd.put(key, toBsonDoc(field.schema().valueSchema(), m.get(key)));
            }
        }
        doc.put(field.name(), bd);
    }

    private void handleArrayField(BsonDocument doc, Struct struct, Field field) {
        logger.trace("handling complex type 'array'");
        BsonArray array = new BsonArray();
        if(struct.get(field)==null) {
            logger.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }
        for(Object element : (List)struct.get(field)) {
            if(field.schema().valueSchema().type().isPrimitive()) {
                array.add(getConverter(field.schema().valueSchema()).toBson(element,field.schema()));
            } else {
                array.add(toBsonDoc(field.schema().valueSchema(), element));
            }
        }
        doc.put(field.name(), array);
    }

    private void handleStructField(BsonDocument doc, Struct struct, Field field) {
        logger.trace("handling complex type 'struct'");
        if(struct.get(field)!=null) {
            logger.trace(struct.get(field).toString());
            doc.put(field.name(), toBsonDoc(field.schema(), struct.get(field)));
        } else {
            logger.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
        }
    }

    private void handlePrimitiveField(BsonDocument doc, Struct struct, Field field) {
        logger.trace("handling primitive type '{}'",field.schema().type());
        doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field),field.schema()));
    }

    private boolean isSupportedLogicalType(Schema schema) {

        if(schema.name() == null) {
            return false;
        }

        return LOGICAL_TYPE_NAMES.contains(schema.name());

    }

    private SinkFieldConverter getConverter(Schema schema) {

        SinkFieldConverter converter;

        if(isSupportedLogicalType(schema)) {
            converter = logicalConverters.get(schema.name());
        } else {
            converter = converters.get(schema.type());
        }

        if (converter == null) {
            throw new ConnectException("error no registered converter found for " + schema.type().getName());
        }

        return converter;
    }
}
