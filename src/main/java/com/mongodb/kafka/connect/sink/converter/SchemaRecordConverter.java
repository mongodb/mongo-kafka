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
import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.MAP;

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
import org.bson.BsonValue;

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

/** Used for converting Struct based data eg. Avro or JSON with schema */
class SchemaRecordConverter implements RecordConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRecordConverter.class);
  private static final Set<String> LOGICAL_TYPE_NAMES =
      unmodifiableSet(
          new HashSet<>(
              asList(
                  Date.LOGICAL_NAME,
                  Decimal.LOGICAL_NAME,
                  Time.LOGICAL_NAME,
                  Timestamp.LOGICAL_NAME)));

  private final Map<Schema.Type, SinkFieldConverter> converters = new HashMap<>();
  private final Map<String, SinkFieldConverter> logicalConverters = new HashMap<>();

  SchemaRecordConverter() {
    // standard types
    registerSinkFieldConverter(new BooleanFieldConverter());
    registerSinkFieldConverter(new Int8FieldConverter());
    registerSinkFieldConverter(new Int16FieldConverter());
    registerSinkFieldConverter(new Int32FieldConverter());
    registerSinkFieldConverter(new Int64FieldConverter());
    registerSinkFieldConverter(new Float32FieldConverter());
    registerSinkFieldConverter(new Float64FieldConverter());
    registerSinkFieldConverter(new StringFieldConverter());
    registerSinkFieldConverter(new BytesFieldConverter());

    // logical types
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
    return toBsonDoc(schema, value).asDocument();
  }

  private void registerSinkFieldConverter(final SinkFieldConverter converter) {
    converters.put(converter.getSchema().type(), converter);
  }

  private void registerSinkFieldLogicalConverter(final SinkFieldConverter converter) {
    logicalConverters.put(converter.getSchema().name(), converter);
  }

  private BsonValue toBsonDoc(final Schema schema, final Object value) {
    if (value == null) {
      return BsonNull.VALUE;
    }
    BsonDocument doc = new BsonDocument();
    if (schema.type() == MAP) {
      Schema fieldSchema = schema.valueSchema();
      Map m = (Map) value;
      for (Object entry : m.keySet()) {
        String key = (String) entry;
        if (fieldSchema.type().isPrimitive()) {
          doc.put(key, getConverter(fieldSchema).toBson(m.get(key), fieldSchema));
        } else if (fieldSchema.type().equals(ARRAY)) {
          doc.put(key, toBsonArray(fieldSchema, m.get(key)));
        } else {
          if (m.get(key) == null) {
            doc.put(key, BsonNull.VALUE);
          } else {
            doc.put(key, toBsonDoc(fieldSchema, m.get(key)));
          }
        }
      }
    } else {
      schema.fields().forEach(f -> doc.put(f.name(), processField((Struct) value, f)));
    }
    return doc;
  }

  private BsonValue toBsonArray(final Schema schema, final Object value) {
    if (value == null) {
      return BsonNull.VALUE;
    }
    Schema fieldSchema = schema.valueSchema();
    BsonArray bsonArray = new BsonArray();
    List<?> myList = (List) value;
    myList.forEach(
        v -> {
          if (fieldSchema.type().isPrimitive()) {
            if (v == null) {
              bsonArray.add(BsonNull.VALUE);
            } else {
              bsonArray.add(getConverter(fieldSchema).toBson(v));
            }
          } else if (fieldSchema.type().equals(ARRAY)) {
            bsonArray.add(toBsonArray(fieldSchema, v));
          } else {
            bsonArray.add(toBsonDoc(fieldSchema, v));
          }
        });
    return bsonArray;
  }

  private BsonValue processField(final Struct struct, final Field field) {
    LOGGER.trace("processing field '{}'", field.name());

    if (struct.get(field.name()) == null) {
      LOGGER.trace("no field in struct -> adding null");
      return BsonNull.VALUE;
    }

    if (isSupportedLogicalType(field.schema())) {
      return getConverter(field.schema()).toBson(struct.get(field), field.schema());
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
          return handlePrimitiveField(struct, field);
        case STRUCT:
        case MAP:
          return toBsonDoc(field.schema(), struct.get(field));
        case ARRAY:
          return toBsonArray(field.schema(), struct.get(field));
        default:
          throw new DataException("unexpected / unsupported schema type " + field.schema().type());
      }
    } catch (Exception exc) {
      throw new DataException("error while processing field " + field.name(), exc);
    }
  }

  private BsonValue handlePrimitiveField(final Struct struct, final Field field) {
    LOGGER.trace("handling primitive type '{}'", field.schema().type());
    return getConverter(field.schema()).toBson(struct.get(field), field.schema());
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
      throw new ConnectException(
          "error no registered converter found for " + schema.type().getName());
    }
    return converter;
  }
}
