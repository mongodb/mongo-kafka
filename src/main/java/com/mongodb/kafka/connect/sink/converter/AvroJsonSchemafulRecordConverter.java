/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink.converter;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;

import com.mongodb.kafka.connect.sink.converter.types.sink.bson.BooleanFieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.BytesFieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.Float32FieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.Float64FieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.Int16FieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.Int32FieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.Int64FieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.Int8FieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.SinkFieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.StringFieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.logical.DateFieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.logical.DecimalFieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.logical.TimeFieldConverter;
import com.mongodb.kafka.connect.sink.converter.types.sink.bson.logical.TimestampFieldConverter;

//looks like Avro and JSON + Schema is convertible by means of
//a unified conversion approach since they are using the
//same the Struct/Type information ...
class AvroJsonSchemafulRecordConverter implements RecordConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroJsonSchemafulRecordConverter.class);
    private static final Set<String> LOGICAL_TYPE_NAMES = unmodifiableSet(new HashSet<>(
            asList(Date.LOGICAL_NAME, Decimal.LOGICAL_NAME, Time.LOGICAL_NAME, Timestamp.LOGICAL_NAME))
    );

    private final Map<Schema.Type, SinkFieldConverter> converters = new HashMap<>();
    private final Map<String, SinkFieldConverter> logicalConverters = new HashMap<>();


    AvroJsonSchemafulRecordConverter() {
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
    public BsonDocument convert(final Schema schema, final Object value) {

        if (schema == null || value == null) {
            throw new DataException("Error: schema and/or value was null for AVRO conversion");
        }

        return toBsonDoc(schema, value);

    }

    private void registerSinkFieldConverter(final SinkFieldConverter converter) {
        converters.put(converter.getSchema().type(), converter);
    }

    private void registerSinkFieldLogicalConverter(final SinkFieldConverter converter) {
        logicalConverters.put(converter.getSchema().name(), converter);
    }

    private BsonDocument toBsonDoc(final Schema schema, final Object value) {
        BsonDocument doc = new BsonDocument();
        schema.fields().forEach(f -> processField(doc, (Struct) value, f));
        return doc;
    }

    private void processField(final BsonDocument doc, final Struct struct, final Field field) {

        LOGGER.trace("processing field '{}'", field.name());

        if (isSupportedLogicalType(field.schema())) {
            doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field), field.schema()));
            return;
        }

        try {
            switch (field.schema().type()) {
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

    private void handleMapField(final BsonDocument doc, final Struct struct, final Field field) {
        LOGGER.trace("handling complex type 'map'");
        BsonDocument bd = new BsonDocument();
        if (struct.get(field) == null) {
            LOGGER.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }
        Map m = (Map) struct.get(field);
        for (Object entry : m.keySet()) {
            String key = (String) entry;
            if (field.schema().valueSchema().type().isPrimitive()) {
                bd.put(key, getConverter(field.schema().valueSchema()).toBson(m.get(key), field.schema()));
            } else {
                bd.put(key, toBsonDoc(field.schema().valueSchema(), m.get(key)));
            }
        }
        doc.put(field.name(), bd);
    }

    private void handleArrayField(final BsonDocument doc, final Struct struct, final Field field) {
        LOGGER.trace("handling complex type 'array'");
        BsonArray array = new BsonArray();
        if (struct.get(field) == null) {
            LOGGER.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
            return;
        }
        for (Object element : (List) struct.get(field)) {
            if (field.schema().valueSchema().type().isPrimitive()) {
                array.add(getConverter(field.schema().valueSchema()).toBson(element, field.schema()));
            } else {
                array.add(toBsonDoc(field.schema().valueSchema(), element));
            }
        }
        doc.put(field.name(), array);
    }

    private void handleStructField(final BsonDocument doc, final Struct struct, final Field field) {
        LOGGER.trace("handling complex type 'struct'");
        if (struct.get(field) != null) {
            LOGGER.trace(struct.get(field).toString());
            doc.put(field.name(), toBsonDoc(field.schema(), struct.get(field)));
        } else {
            LOGGER.trace("no field in struct -> adding null");
            doc.put(field.name(), BsonNull.VALUE);
        }
    }

    private void handlePrimitiveField(final BsonDocument doc, final Struct struct, final Field field) {
        LOGGER.trace("handling primitive type '{}'", field.schema().type());
        doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field), field.schema()));
    }

    private boolean isSupportedLogicalType(final Schema schema) {
        if (schema.name() == null) {
            return false;
        }
        return LOGICAL_TYPE_NAMES.contains(schema.name());
    }

    private SinkFieldConverter getConverter(final Schema schema) {
        SinkFieldConverter converter;

        if (isSupportedLogicalType(schema)) {
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
