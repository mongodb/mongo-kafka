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

import static com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue.documentToByteArray;
import static com.mongodb.kafka.connect.source.schema.SchemaUtils.assertSchemaAndValueEquals;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.RawBsonDocument;
import org.bson.types.Decimal128;

import com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson;

@RunWith(JUnitPlatform.class)
public class BsonValueToSchemaAndValueTest {

  private static final RawBsonDocument BSON_DOCUMENT =
      RawBsonDocument.parse(
          "{\"_id\": {\"$oid\": \"5f15aab12435743f9bd126a4\"},"
              + " \"myString\": \"some foo bla text\","
              + " \"myInt\": {\"$numberInt\": \"42\"},"
              + " \"myDouble\": {\"$numberDouble\": \"20.21\"},"
              + " \"mySubDoc\": {\"A\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
              + " \"B\": {\"$date\": {\"$numberLong\": \"1577863627000\"}},"
              + " \"C\": {\"D\": \"12345.6789\"}},"
              + " \"myArray\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
              + " \"myBytes\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
              + " \"myDate\": {\"$date\": {\"$numberLong\": \"1234567890\"}},"
              + " \"myDecimal\": {\"$numberDecimal\": \"12345.6789\"}"
              + "}");

  private static final BsonValueToSchemaAndValue CONVERTER =
      new BsonValueToSchemaAndValue(new SimplifiedJson().getJsonWriterSettings());

  @Test
  @DisplayName("test string support")
  void testStringSupport() {
    Map<String, String> expected =
        new HashMap<String, String>() {
          {
            put("_id", "5f15aab12435743f9bd126a4");
            put("myString", "some foo bla text");
            put("myInt", "42");
            put("myDouble", "20.21");
            put(
                "mySubDoc",
                "{\"A\": \"S2Fma2Egcm9ja3Mh\", "
                    + "\"B\": \"2020-01-01T07:27:07Z\", "
                    + "\"C\": {\"D\": \"12345.6789\"}}");
            put("myArray", "[1, 2, 3]");
            put("myBytes", "S2Fma2Egcm9ja3Mh");
            put("myDate", "1970-01-15T06:56:07.89Z");
            put("myDecimal", "12345.6789");
          }
        };

    BSON_DOCUMENT.forEach(
        (k, v) -> {
          assertSchemaAndValueEquals(
              new SchemaAndValue(Schema.STRING_SCHEMA, expected.get(k)),
              CONVERTER.toSchemaAndValue(Schema.STRING_SCHEMA, v));
        });
  }

  @Test
  @DisplayName("test number support")
  void testNumberSupport() {
    BsonInt32 bsonInt32 = new BsonInt32(42);
    BsonInt64 bsonInt64 = new BsonInt64(2020L);
    BsonDouble bsonDouble = new BsonDouble(20.20);

    assertAll(
        "Testing int8 support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 42),
                CONVERTER.toSchemaAndValue(Schema.INT8_SCHEMA, bsonInt32)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 2020),
                CONVERTER.toSchemaAndValue(Schema.INT8_SCHEMA, bsonInt64)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 20.20),
                CONVERTER.toSchemaAndValue(Schema.INT8_SCHEMA, bsonDouble)));

    assertAll(
        "Testing int16 support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT16_SCHEMA, (short) 42),
                CONVERTER.toSchemaAndValue(Schema.INT16_SCHEMA, bsonInt32)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT16_SCHEMA, (short) 2020),
                CONVERTER.toSchemaAndValue(Schema.INT16_SCHEMA, bsonInt64)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT16_SCHEMA, (short) 20.20),
                CONVERTER.toSchemaAndValue(Schema.INT16_SCHEMA, bsonDouble)));

    assertAll(
        "Testing int32 support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT32_SCHEMA, 42),
                CONVERTER.toSchemaAndValue(Schema.INT32_SCHEMA, bsonInt32)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT32_SCHEMA, 2020),
                CONVERTER.toSchemaAndValue(Schema.INT32_SCHEMA, bsonInt64)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT32_SCHEMA, (int) 20.20),
                CONVERTER.toSchemaAndValue(Schema.INT32_SCHEMA, bsonDouble)));

    assertAll(
        "Testing int64 support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT64_SCHEMA, 42L),
                CONVERTER.toSchemaAndValue(Schema.INT64_SCHEMA, bsonInt32)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT64_SCHEMA, 2020L),
                CONVERTER.toSchemaAndValue(Schema.INT64_SCHEMA, bsonInt64)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.INT64_SCHEMA, (long) 20.20),
                CONVERTER.toSchemaAndValue(Schema.INT64_SCHEMA, bsonDouble)));

    assertAll(
        "Testing float32 support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.FLOAT32_SCHEMA, (float) 42),
                CONVERTER.toSchemaAndValue(Schema.FLOAT32_SCHEMA, bsonInt32)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.FLOAT32_SCHEMA, (float) 2020L),
                CONVERTER.toSchemaAndValue(Schema.FLOAT32_SCHEMA, bsonInt64)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.FLOAT32_SCHEMA, (float) 20.20),
                CONVERTER.toSchemaAndValue(Schema.FLOAT32_SCHEMA, bsonDouble)));

    assertAll(
        "Testing float64 support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.FLOAT64_SCHEMA, (double) 42),
                CONVERTER.toSchemaAndValue(Schema.FLOAT64_SCHEMA, bsonInt32)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.FLOAT64_SCHEMA, (double) 2020),
                CONVERTER.toSchemaAndValue(Schema.FLOAT64_SCHEMA, bsonInt64)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.FLOAT64_SCHEMA, 20.20),
                CONVERTER.toSchemaAndValue(Schema.FLOAT64_SCHEMA, bsonDouble)));

    assertAll(
        "Testing logical types",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Date.SCHEMA, Date.toLogical(Date.SCHEMA, 2020)),
                CONVERTER.toSchemaAndValue(Date.SCHEMA, new BsonDateTime(2020L))),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Time.SCHEMA, Time.toLogical(Time.SCHEMA, 2020)),
                CONVERTER.toSchemaAndValue(Time.SCHEMA, new BsonDateTime(2020L))),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Timestamp.SCHEMA, Timestamp.toLogical(Timestamp.SCHEMA, 2020)),
                CONVERTER.toSchemaAndValue(Timestamp.SCHEMA, new BsonDateTime(2020L))));

    List<String> validKeys = asList("myInt", "myDouble", "myDate", "myDecimal");
    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !validKeys.contains(k))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERTER.toSchemaAndValue(Schema.INT64_SCHEMA, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test boolean support")
  void testBooleanSupport() {
    Schema schema = Schema.BOOLEAN_SCHEMA;
    assertAll(
        "Testing boolean support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(schema, true),
                CONVERTER.toSchemaAndValue(schema, BsonBoolean.TRUE)),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(schema, false),
                CONVERTER.toSchemaAndValue(schema, BsonBoolean.FALSE)));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("myBoolean"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test bytes support")
  void testBytesSupport() {
    Schema schema = Schema.BYTES_SCHEMA;

    assertAll(
        "Testing bytes support",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(
                    schema,
                    BSON_DOCUMENT
                        .getString("myString")
                        .getValue()
                        .getBytes(StandardCharsets.UTF_8)),
                CONVERTER.toSchemaAndValue(Schema.BYTES_SCHEMA, BSON_DOCUMENT.get("myString"))),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(schema, BSON_DOCUMENT.getBinary("myBytes").getData()),
                CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.getBinary("myBytes"))),
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(Schema.BYTES_SCHEMA, documentToByteArray(BSON_DOCUMENT)),
                CONVERTER.toSchemaAndValue(Schema.BYTES_SCHEMA, BSON_DOCUMENT)));

    BsonDecimal128 decimal128 = new BsonDecimal128(Decimal128.parse("101"));
    Schema decimalSchema = Decimal.schema(0);
    assertAll(
        "Testing logical types",
        () ->
            assertSchemaAndValueEquals(
                new SchemaAndValue(decimalSchema, decimal128.decimal128Value().bigDecimalValue()),
                CONVERTER.toSchemaAndValue(decimalSchema, decimal128)));

    List<String> validKeys = asList("myString", "myBytes", "mySubDoc");
    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !validKeys.contains(k))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERTER.toSchemaAndValue(Schema.BYTES_SCHEMA, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test array support")
  void testArraySupport() {
    Schema schema = SchemaBuilder.array(Schema.INT32_SCHEMA).build();

    assertSchemaAndValueEquals(
        new SchemaAndValue(schema, asList(1, 2, 3)),
        CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.get("myArray")));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("myArray"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));
  }

  @Test
  @DisplayName("test Map support")
  void testMapSupport() {
    Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);
    assertSchemaAndValueEquals(
        new SchemaAndValue(
            schema,
            new LinkedHashMap<String, Object>() {
              {
                put("A", "S2Fma2Egcm9ja3Mh");
                put("B", "2020-01-01T07:27:07Z");
                put("C", "{\"D\": \"12345.6789\"}");
              }
            }),
        CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.get("mySubDoc")));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("mySubDoc"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));

    assertThrows(
        ConnectException.class,
        () ->
            CONVERTER.toSchemaAndValue(
                SchemaBuilder.map(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA), BSON_DOCUMENT));
  }

  @Test
  @DisplayName("test struct support")
  void testStructSupport() {
    Schema schema =
        SchemaBuilder.struct()
            .field("A", Schema.STRING_SCHEMA)
            .field("B", SchemaBuilder.string().defaultValue("MISSING"))
            .field("C", SchemaBuilder.struct().field("D", Schema.STRING_SCHEMA).build())
            .build();

    assertSchemaAndValueEquals(
        new SchemaAndValue(
            schema,
            new Struct(schema)
                .put("A", "S2Fma2Egcm9ja3Mh")
                .put("B", "2020-01-01T07:27:07Z")
                .put("C", new Struct(schema.field("C").schema()).put("D", "12345.6789"))),
        CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.get("mySubDoc")));

    Schema schemaWithFieldLookup =
        SchemaBuilder.struct().field("mySubDoc.C.D", Schema.STRING_SCHEMA).build();
    assertSchemaAndValueEquals(
        new SchemaAndValue(
            schemaWithFieldLookup,
            new Struct(schemaWithFieldLookup).put("mySubDoc.C.D", "12345.6789")),
        CONVERTER.toSchemaAndValue(schemaWithFieldLookup, BSON_DOCUMENT));

    Schema schemaWithDefaultValue =
        SchemaBuilder.struct()
            .field("myValue", SchemaBuilder.string().defaultValue("MISSING"))
            .build();
    assertSchemaAndValueEquals(
        new SchemaAndValue(
            schemaWithDefaultValue, new Struct(schemaWithDefaultValue).put("myValue", "MISSING")),
        CONVERTER.toSchemaAndValue(schemaWithDefaultValue, BSON_DOCUMENT));

    Set<String> invalidKeys =
        BSON_DOCUMENT.keySet().stream()
            .filter(k -> !k.equals("mySubDoc"))
            .collect(Collectors.toSet());

    invalidKeys.forEach(
        k ->
            assertThrows(
                DataException.class,
                () -> CONVERTER.toSchemaAndValue(schema, BSON_DOCUMENT.get(k)),
                format("Expected %s to fail", k)));

    BsonDocument invalidDoc = BsonDocument.parse(BSON_DOCUMENT.getDocument("mySubDoc").toJson());
    invalidDoc.remove("A");
    assertThrows(DataException.class, () -> CONVERTER.toSchemaAndValue(schema, invalidDoc));
  }
}
