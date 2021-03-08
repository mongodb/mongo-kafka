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

import static com.mongodb.kafka.connect.util.BsonDocumentFieldLookup.fieldLookup;
import static java.lang.String.format;
import static org.apache.kafka.connect.data.Values.convertToByte;
import static org.apache.kafka.connect.data.Values.convertToDate;
import static org.apache.kafka.connect.data.Values.convertToDouble;
import static org.apache.kafka.connect.data.Values.convertToFloat;
import static org.apache.kafka.connect.data.Values.convertToInteger;
import static org.apache.kafka.connect.data.Values.convertToLong;
import static org.apache.kafka.connect.data.Values.convertToShort;
import static org.apache.kafka.connect.data.Values.convertToTime;
import static org.apache.kafka.connect.data.Values.convertToTimestamp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
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
    if (schema.isOptional() && bsonValue.isNull()) {
      return new SchemaAndValue(schema, null);
    }

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
      int startPosition = byteBuffer.position();
      int length = byteBuffer.limit() - startPosition;
      byte[] byteArray = new byte[length];
      System.arraycopy(byteBuffer.array(), startPosition, byteArray, 0, length);
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
    Object value = null;
    if (bsonValue.isNumber()) {
      BsonNumber bsonNumber = bsonValue.asNumber();
      if (bsonNumber.isInt32()) {
        value = bsonNumber.intValue();
      } else if (bsonNumber.isInt64()) {
        value = bsonNumber.longValue();
      } else if (bsonNumber.isDouble()) {
        value = bsonNumber.doubleValue();
      }
    } else if (bsonValue.isTimestamp()) {
      value = bsonValue.asTimestamp().getTime() * 1000; // normalize to millis
    } else if (bsonValue.isDateTime()) {
      value = bsonValue.asDateTime().getValue();
    } else {
      throw unexpectedBsonValueType(schema.type(), bsonValue);
    }

    switch (schema.type()) {
      case INT8:
        value = convertToByte(schema, value);
        break;
      case INT16:
        value = convertToShort(schema, value);
        break;
      case INT32:
        value = convertToInteger(schema, value);
        break;
      case INT64:
        value = convertToLong(schema, value);
        break;
      case FLOAT32:
        value = convertToFloat(schema, value);
        break;
      case FLOAT64:
        value = convertToDouble(schema, value);
        break;
      default:
        throw unexpectedBsonValueType(schema.type(), bsonValue);
    }

    if (schema.name() != null) {
      switch (schema.name()) {
        case Time.LOGICAL_NAME:
          value = convertToTime(schema, value);
          break;
        case Date.LOGICAL_NAME:
          value = convertToDate(schema, value);
          break;
        case Timestamp.LOGICAL_NAME:
          value = convertToTimestamp(schema, value);
          break;
        default:
          // do nothing
          break;
      }
    }
    return new SchemaAndValue(schema, value);
  }

  private SchemaAndValue stringToSchemaAndValue(final Schema schema, final BsonValue bsonValue) {
    String value;
    if (bsonValue.isString()) {
      value = bsonValue.asString().getValue();
    } else if (bsonValue.isDocument()) {
      value = bsonValue.asDocument().toJson(jsonWriterSettings);
    } else {
      value = new BsonDocument("v", bsonValue).toJson(jsonWriterSettings);
      // Strip down to just the value
      value = value.substring(6, value.length() - 1);
      // Remove unnecessary quotes of BsonValues converted to Strings.
      // Such as BsonBinary base64 string representations
      if (value.startsWith("\"") && value.endsWith("\"")) {
        value = value.substring(1, value.length() - 1);
      }
    }
    return new SchemaAndValue(schema, value);
  }

  private SchemaAndValue bytesToSchemaAndValue(final Schema schema, final BsonValue bsonValue) {
    Object value = null;
    switch (bsonValue.getBsonType()) {
      case STRING:
        value = bsonValue.asString().getValue().getBytes(StandardCharsets.UTF_8);
        break;
      case BINARY:
        value = bsonValue.asBinary().getData();
        break;
      case DECIMAL128:
        if (Objects.equals(schema.name(), Decimal.LOGICAL_NAME)) {
          value = bsonValue.asDecimal128().getValue().bigDecimalValue();
        }
        break;
      case DOCUMENT:
        value = documentToByteArray(bsonValue.asDocument());
        break;
      default:
        // ignore
        break;
    }

    if (value == null) {
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
              Optional<BsonValue> fieldValue = fieldLookup(f.name(), document);
              if (fieldValue.isPresent()) {
                SchemaAndValue schemaAndValue = toSchemaAndValue(f.schema(), fieldValue.get());
                structValue.put(f, schemaAndValue.value());
              } else {
                boolean optionalField = f.schema().isOptional();
                Object defaultValue = f.schema().defaultValue();
                if (optionalField || defaultValue != null) {
                  structValue.put(f, defaultValue);
                } else {
                  throw missingFieldException(f, document);
                }
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
            type.getName(), value.getBsonType().toString().toLowerCase(Locale.ROOT)));
  }

  private DataException missingFieldException(final Field field, final BsonDocument value) {
    return new DataException(
        format("Missing field '%s' in: '%s'", field.name(), value.toJson(jsonWriterSettings)));
  }
}
