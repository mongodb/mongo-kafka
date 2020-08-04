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
 */

package com.mongodb.kafka.connect.source.schema;

import static java.lang.String.format;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonNumber;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonValueCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonWriterSettings;

public class BsonValueToSchemaAndValue {
  private static final Codec<BsonValue> BSON_VALUE_CODEC = new BsonValueCodec();
  private final JsonWriterSettings jsonWriterSettings;

  public BsonValueToSchemaAndValue(final JsonWriterSettings jsonWriterSettings) {
    this.jsonWriterSettings = jsonWriterSettings;
  }

  public SchemaAndValue toSchemaAndValue(final Schema schema, final BsonValue bsonValue) {
    SchemaAndValue schemaAndValue;
    switch (schema.type()) {
      case INT8:
      case INT16:
      case INT32:
      case INT64:
      case FLOAT32:
      case FLOAT64:
        schemaAndValue = numberToSchemaAndValue(schema, bsonValue);
        break;
      case BOOLEAN:
        schemaAndValue = booleanToSchemaAndValue(schema, bsonValue);
        break;
      case STRING:
        schemaAndValue = stringToSchemaAndValue(schema, bsonValue);
        break;
      case BYTES:
        schemaAndValue = bytesToSchemaAndValue(schema, bsonValue);
        break;
      case ARRAY:
        schemaAndValue = arrayToSchemaAndValue(schema, bsonValue);
        break;
      case MAP:
        schemaAndValue = mapToSchemaAndValue(schema, bsonValue);
        break;
      case STRUCT:
        schemaAndValue = recordToSchemaAndValue(schema, bsonValue);
        break;
      default:
        throw unsupportedSchemaType(schema);
    }

    return schemaAndValue;
  }

  public static byte[] documentToByteArray(final BsonDocument document) {
    if (document instanceof RawBsonDocument) {
      RawBsonDocument rawBsonDocument = (RawBsonDocument) document;
      ByteBuffer byteBuffer = rawBsonDocument.getByteBuffer().asNIO();
      byte[] byteArray = new byte[byteBuffer.limit()];
      System.arraycopy(
          rawBsonDocument.getByteBuffer().array(), 0, byteArray, 0, byteBuffer.limit());
      return byteArray;
    } else {
      BasicOutputBuffer buffer = new BasicOutputBuffer();
      try (BsonBinaryWriter writer = new BsonBinaryWriter(buffer)) {
        BSON_VALUE_CODEC.encode(writer, document, EncoderContext.builder().build());
      }
      return buffer.toByteArray();
    }
  }

  private SchemaAndValue numberToSchemaAndValue(final Schema schema, final BsonValue bsonValue) {
    if (!bsonValue.isNumber()) {
      throw unexpectedBsonValueType(schema.type(), bsonValue);
    }
    BsonNumber bsonNumber = bsonValue.asNumber();
    Object value;
    switch (schema.type()) {
      case INT8:
        value = (byte) bsonNumber.intValue();
        break;
      case INT16:
        value = (short) bsonNumber.intValue();
        break;
      case INT32:
        value = bsonNumber.intValue();
        break;
      case INT64:
        value = bsonNumber.longValue();
        break;
      case FLOAT32:
        value = (float) bsonNumber.doubleValue();
        break;
      case FLOAT64:
        value = bsonNumber.doubleValue();
        break;
      default:
        throw unexpectedBsonValueType(schema.type(), bsonValue);
    }
    return new SchemaAndValue(schema, value);
  }

  private SchemaAndValue stringToSchemaAndValue(final Schema schema, final BsonValue bsonValue) {
    String value;
    if (bsonValue.isString()) {
      value = bsonValue.asString().getValue();
    } else {
      value = new BsonDocument("v", bsonValue).toJson(jsonWriterSettings);
      // Strip down to just the value
      value = value.substring(6, value.length() - 1);
      // Remove unneccessary quotes of BsonValues converted to Strings.
      if (value.startsWith("\"") && value.endsWith("\"")) {
        value = value.substring(1, value.length() - 1);
      }
    }
    return new SchemaAndValue(schema, value);
  }

  private SchemaAndValue bytesToSchemaAndValue(final Schema schema, final BsonValue bsonValue) {
    byte[] value;
    switch (bsonValue.getBsonType()) {
      case STRING:
        value = bsonValue.asString().getValue().getBytes(StandardCharsets.UTF_8);
        break;
      case BINARY:
        value = bsonValue.asBinary().getData();
        break;
      case DOCUMENT:
        value = documentToByteArray(bsonValue.asDocument());
        break;
      default:
        throw unexpectedBsonValueType(Type.BYTES, bsonValue);
    }
    return new SchemaAndValue(schema, value);
  }

  private SchemaAndValue arrayToSchemaAndValue(final Schema schema, final BsonValue value) {
    if (!value.isArray()) {
      throw unexpectedBsonValueType(Schema.Type.ARRAY, value);
    }
    List<Object> values = new ArrayList<>();
    value.asArray().forEach(v -> values.add(toSchemaAndValue(schema.valueSchema(), v).value()));
    return new SchemaAndValue(schema, values);
  }

  private SchemaAndValue mapToSchemaAndValue(final Schema schema, final BsonValue value) {
    if (!value.isDocument()) {
      throw unexpectedBsonValueType(schema.type(), value);
    }

    if (schema.keySchema() != Schema.STRING_SCHEMA) {
      throw unsupportedSchemaType(schema, ". Unexpected key schema type.");
    }

    BsonDocument document = value.asDocument();
    Map<String, Object> mapValue = new LinkedHashMap<>();
    document.forEach((k, v) -> mapValue.put(k, toSchemaAndValue(schema.valueSchema(), v).value()));
    return new SchemaAndValue(schema, mapValue);
  }

  private SchemaAndValue recordToSchemaAndValue(final Schema schema, final BsonValue value) {
    if (!value.isDocument()) {
      throw unexpectedBsonValueType(schema.type(), value);
    }
    BsonDocument document = value.asDocument();

    Struct structValue = new Struct(schema);
    schema
        .fields()
        .forEach(
            f -> {
              boolean optionalField = f.schema().isOptional();
              if (document.containsKey(f.name())) {
                SchemaAndValue schemaAndValue =
                    toSchemaAndValue(f.schema(), document.get(f.name()));
                structValue.put(f, schemaAndValue.value());
              } else if (optionalField && f.schema().defaultValue() != null) {
                structValue.put(f, null);
              } else if (!optionalField) {
                throw missingFieldException(f, document);
              }
            });
    return new SchemaAndValue(schema, structValue);
  }

  private SchemaAndValue booleanToSchemaAndValue(final Schema schema, final BsonValue bsonValue) {
    if (!bsonValue.isBoolean()) {
      throw unexpectedBsonValueType(Schema.Type.BOOLEAN, bsonValue);
    }
    return new SchemaAndValue(schema, bsonValue.asBoolean().getValue());
  }

  private ConnectException unsupportedSchemaType(final Schema schema) {
    return unsupportedSchemaType(schema, "");
  }

  private ConnectException unsupportedSchemaType(final Schema schema, final String extra) {
    return new ConnectException(format("Unsupported Schema type: %s %s", schema.type(), extra));
  }

  private DataException unexpectedBsonValueType(final Schema.Type type, final BsonValue value) {
    return new DataException(
        format(
            "Schema type of %s but value was of type: %s",
            type.getName(), value.getBsonType().toString().toLowerCase()));
  }

  private DataException missingFieldException(final Field field, final BsonDocument value) {
    return new DataException(
        format("Missing field '%s' in: '%s'", field.name(), value.toJson(jsonWriterSettings)));
  }
}
