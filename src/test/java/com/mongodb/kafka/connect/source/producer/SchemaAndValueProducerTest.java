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

package com.mongodb.kafka.connect.source.producer;

import static com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults.DEFAULT_AVRO_KEY_SCHEMA;
import static com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults.DEFAULT_AVRO_VALUE_SCHEMA;
import static com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults.DEFAULT_KEY_SCHEMA;
import static com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults.DEFAULT_VALUE_SCHEMA;
import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.generateName;
import static com.mongodb.kafka.connect.source.schema.SchemaUtils.assertSchemaAndValueEquals;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.util.Base64;
import java.util.Date;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.json.JsonWriterSettings;

import com.mongodb.kafka.connect.source.json.formatter.ExtendedJson;
import com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson;

@RunWith(JUnitPlatform.class)
public class SchemaAndValueProducerTest {

  private static final String FULL_DOCUMENT_JSON =
      "{"
          + "\"arrayEmpty\": [], "
          + "\"arraySimple\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}], "
          + "\"arrayComplex\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": {\"$numberInt\": \"2\"}}], "
          + "\"arrayMixedTypes\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, true,"
          + " [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
          + " {\"a\": {\"$numberInt\": \"2\"}}], "
          + "\"arrayComplexMixedTypes\": [{\"a\": {\"$numberInt\": \"1\"}}, {\"a\": \"a\"}], "
          + "\"binary\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}}, "
          + "\"boolean\": true, "
          + "\"code\": {\"$code\": \"int i = 0;\"}, "
          + "\"codeWithScope\": {\"$code\": \"int x = y\", \"$scope\": {\"y\": {\"$numberInt\": \"1\"}}}, "
          + "\"dateTime\": {\"$date\": {\"$numberLong\": \"1577836801000\"}}, "
          + "\"decimal128\": {\"$numberDecimal\": \"1.0\"}, "
          + "\"document\": {\"a\": {\"$numberInt\": \"1\"}}, "
          + "\"double\": {\"$numberDouble\": \"62.0\"}, "
          + "\"int32\": {\"$numberInt\": \"42\"}, "
          + "\"int64\": {\"$numberLong\": \"52\"}, "
          + "\"maxKey\": {\"$maxKey\": 1}, "
          + "\"minKey\": {\"$minKey\": 1}, "
          + "\"null\": null, "
          + "\"objectId\": {\"$oid\": \"5f3d1bbde0ca4d2829c91e1d\"}, "
          + "\"regex\": {\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}, "
          + "\"string\": \"the fox ...\", "
          + "\"symbol\": {\"$symbol\": \"ruby stuff\"}, "
          + "\"timestamp\": {\"$timestamp\": {\"t\": 305419896, \"i\": 5}}, "
          + "\"undefined\": {\"$undefined\": true}"
          + "}";

  private static final String SIMPLIFIED_FULL_DOCUMENT_JSON =
      "{"
          + "\"arrayEmpty\": [], "
          + "\"arraySimple\": [1, 2, 3], "
          + "\"arrayComplex\": [{\"a\": 1}, {\"a\": 2}], "
          + "\"arrayMixedTypes\": [1, 2, true, [1, 2, 3], {\"a\": 2}], "
          + "\"arrayComplexMixedTypes\": [{\"a\": 1}, {\"a\": \"a\"}], "
          + "\"binary\": \"S2Fma2Egcm9ja3Mh\", "
          + "\"boolean\": true, "
          + "\"code\": {\"$code\": \"int i = 0;\"}, "
          + "\"codeWithScope\": {\"$code\": \"int x = y\", \"$scope\": {\"y\": 1}}, "
          + "\"dateTime\": \"2020-01-01T00:00:01Z\", "
          + "\"decimal128\": \"1.0\", "
          + "\"document\": {\"a\": 1}, "
          + "\"double\": 62.0, "
          + "\"int32\": 42, "
          + "\"int64\": 52, "
          + "\"maxKey\": {\"$maxKey\": 1}, "
          + "\"minKey\": {\"$minKey\": 1}, "
          + "\"null\": null, "
          + "\"objectId\": \"5f3d1bbde0ca4d2829c91e1d\", "
          + "\"regex\": {\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}, "
          + "\"string\": \"the fox ...\", "
          + "\"symbol\": \"ruby stuff\", "
          + "\"timestamp\": {\"$timestamp\": {\"t\": 305419896, \"i\": 5}}, "
          + "\"undefined\": {\"$undefined\": true}"
          + "}";

  private static final String CHANGE_STREAM_DOCUMENT_JSON = generateJson(false);
  private static final String SIMPLIFIED_CHANGE_STREAM_DOCUMENT_JSON = generateJson(true);

  private static final BsonDocument CHANGE_STREAM_DOCUMENT =
      BsonDocument.parse(CHANGE_STREAM_DOCUMENT_JSON);

  private static final JsonWriterSettings SIMPLE_JSON_WRITER_SETTINGS =
      new SimplifiedJson().getJsonWriterSettings();

  private static final JsonWriterSettings EXTENDED_JSON_WRITER_SETTINGS =
      new ExtendedJson().getJsonWriterSettings();

  @Test
  @DisplayName("test avro schema and value producer")
  void testAvroSchemaAndValueProducer() {
    // Test key
    SchemaAndValue expectedKey =
        new SchemaAndValue(
            DEFAULT_KEY_SCHEMA,
            new Struct(DEFAULT_KEY_SCHEMA).put("_id", "{\"_data\": \"5f15aab12435743f9bd126a4\"}"));
    SchemaAndValueProducer keyProducer =
        new AvroSchemaAndValueProducer(DEFAULT_AVRO_KEY_SCHEMA, SIMPLE_JSON_WRITER_SETTINGS);
    assertSchemaAndValueEquals(expectedKey, keyProducer.get(CHANGE_STREAM_DOCUMENT));

    // Test Value - simple Json
    SchemaAndValue expectedValue =
        new SchemaAndValue(DEFAULT_VALUE_SCHEMA, generateExpectedValue(true));
    SchemaAndValueProducer valueProducer =
        new AvroSchemaAndValueProducer(DEFAULT_AVRO_VALUE_SCHEMA, SIMPLE_JSON_WRITER_SETTINGS);
    assertSchemaAndValueEquals(expectedValue, valueProducer.get(CHANGE_STREAM_DOCUMENT));

    // Test Value - extended Json
    SchemaAndValue expectedExtendedValue =
        new SchemaAndValue(DEFAULT_VALUE_SCHEMA, generateExpectedValue(false));
    SchemaAndValueProducer extendedJsonValueProducer =
        new AvroSchemaAndValueProducer(DEFAULT_AVRO_VALUE_SCHEMA, EXTENDED_JSON_WRITER_SETTINGS);
    assertSchemaAndValueEquals(
        expectedExtendedValue, extendedJsonValueProducer.get(CHANGE_STREAM_DOCUMENT));
  }

  @Test
  @DisplayName("test infer schema and value producer")
  void testInferSchemaAndValueProducer() {

    Schema expectedSchema =
        nameAndBuildSchema(
            SchemaBuilder.struct()
                .field(
                    "arrayComplex",
                    SchemaBuilder.array(
                            nameAndBuildOptionalSchema(
                                SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT32_SCHEMA)))
                        .optional()
                        .build())
                .field(
                    "arrayComplexMixedTypes",
                    SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .field(
                    "arrayEmpty",
                    SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .field(
                    "arrayMixedTypes",
                    SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .field(
                    "arraySimple",
                    SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build())
                .field("binary", Schema.OPTIONAL_BYTES_SCHEMA)
                .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("code", Schema.OPTIONAL_STRING_SCHEMA)
                .field("codeWithScope", Schema.OPTIONAL_STRING_SCHEMA)
                .field("dateTime", Timestamp.builder().optional().build())
                .field("decimal128", Decimal.builder(1).optional().build())
                .field(
                    "document",
                    nameAndBuildOptionalSchema(
                        SchemaBuilder.struct().field("a", Schema.OPTIONAL_INT32_SCHEMA)))
                .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("int32", Schema.OPTIONAL_INT32_SCHEMA)
                .field("int64", Schema.OPTIONAL_INT64_SCHEMA)
                .field("maxKey", Schema.OPTIONAL_STRING_SCHEMA)
                .field("minKey", Schema.OPTIONAL_STRING_SCHEMA)
                .field("null", Schema.OPTIONAL_STRING_SCHEMA)
                .field("objectId", Schema.OPTIONAL_STRING_SCHEMA)
                .field("regex", Schema.OPTIONAL_STRING_SCHEMA)
                .field("string", Schema.OPTIONAL_STRING_SCHEMA)
                .field("symbol", Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", Timestamp.builder().optional().build())
                .field("undefined", Schema.OPTIONAL_STRING_SCHEMA));

    Schema arrayComplexValueSchema = expectedSchema.field("arrayComplex").schema().valueSchema();
    Schema documentSchema = expectedSchema.field("document").schema();

    SchemaAndValue expectedValue =
        new SchemaAndValue(
            expectedSchema,
            new Struct(expectedSchema)
                .put("arrayEmpty", emptyList())
                .put("arraySimple", asList(1, 2, 3))
                .put(
                    "arrayComplex",
                    asList(
                        new Struct(arrayComplexValueSchema).put("a", 1),
                        new Struct(arrayComplexValueSchema).put("a", 2)))
                .put("arrayMixedTypes", asList("1", "2", "true", "[1, 2, 3]", "{\"a\": 2}"))
                .put("arrayComplexMixedTypes", asList("{\"a\": 1}", "{\"a\": \"a\"}"))
                .put("binary", Base64.getDecoder().decode("S2Fma2Egcm9ja3Mh"))
                .put("boolean", true)
                .put("code", "{\"$code\": \"int i = 0;\"}")
                .put("codeWithScope", "{\"$code\": \"int x = y\", \"$scope\": {\"y\": 1}}")
                .put("dateTime", new Date(1577836801000L))
                .put("decimal128", BigDecimal.valueOf(1.0))
                .put("document", new Struct(documentSchema).put("a", 1))
                .put("double", 62.0)
                .put("int32", 42)
                .put("int64", 52L)
                .put("maxKey", "{\"$maxKey\": 1}")
                .put("minKey", "{\"$minKey\": 1}")
                .put("null", null)
                .put("objectId", "5f3d1bbde0ca4d2829c91e1d")
                .put(
                    "regex",
                    "{\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}")
                .put("string", "the fox ...")
                .put("symbol", "ruby stuff")
                .put("timestamp", new Date(477217984))
                .put("undefined", "{\"$undefined\": true}"));

    SchemaAndValueProducer valueProducer =
        new InferSchemaAndValueProducer(SIMPLE_JSON_WRITER_SETTINGS);

    assertSchemaAndValueEquals(
        expectedValue, valueProducer.get(BsonDocument.parse(FULL_DOCUMENT_JSON)));
  }

  @Test
  @DisplayName("test raw json string schema and value producer")
  void testRawJsonStringSchemaAndValueProducer() {
    assertEquals(
        new SchemaAndValue(Schema.STRING_SCHEMA, SIMPLIFIED_CHANGE_STREAM_DOCUMENT_JSON),
        new RawJsonStringSchemaAndValueProducer(SIMPLE_JSON_WRITER_SETTINGS)
            .get(CHANGE_STREAM_DOCUMENT));
  }

  @Test
  @DisplayName("test bson schema and value producer")
  void testBsonSchemaAndValueProducer() {
    SchemaAndValue actual = new BsonSchemaAndValueProducer().get(CHANGE_STREAM_DOCUMENT);
    assertAll(
        "Assert schema and value matches",
        () -> assertEquals(Schema.BYTES_SCHEMA.schema(), actual.schema()),
        // Ensure the data length is truncated.
        () -> assertEquals(1071, ((byte[]) actual.value()).length),
        () -> assertEquals(CHANGE_STREAM_DOCUMENT, new RawBsonDocument((byte[]) actual.value())));
  }

  static Struct generateExpectedValue(final boolean simplified) {
    return new Struct(DEFAULT_VALUE_SCHEMA) {
      {
        put("_id", "{\"_data\": \"5f15aab12435743f9bd126a4\"}");
        put("operationType", "<operation>");
        put("fullDocument", getFullDocument(simplified));
        put(
            "ns",
            new Struct(DEFAULT_VALUE_SCHEMA.field("ns").schema()) {
              {
                put("db", "<database>");
                put("coll", "<collection>");
              }
            });
        put(
            "to",
            new Struct(DEFAULT_VALUE_SCHEMA.field("to").schema()) {
              {
                put("db", "<to_database>");
                put("coll", "<to_collection>");
              }
            });
        put("documentKey", getDocumentKey(simplified));
        put(
            "updateDescription",
            new Struct(DEFAULT_VALUE_SCHEMA.field("updateDescription").schema()) {
              {
                put("updatedFields", getUpdatedField(simplified));
                put("removedFields", singletonList("legacyUUID"));
              }
            });
        put("clusterTime", "{\"$timestamp\": {\"t\": 123456789, \"i\": 42}}");
        put("txnNumber", 987654321L);
        put(
            "lsid",
            new Struct(DEFAULT_VALUE_SCHEMA.field("lsid").schema()) {
              {
                put("id", getLsidId(simplified));
                put("uid", getLsidUid(simplified));
              }
            });
      }
    };
  }

  static Schema nameAndBuildSchema(final SchemaBuilder builder) {
    return builder.name(generateName(builder)).build();
  }

  static Schema nameAndBuildOptionalSchema(final SchemaBuilder builder) {
    return builder.name(generateName(builder)).optional().build();
  }

  static String getFullDocument(final boolean simplified) {
    return simplified ? SIMPLIFIED_FULL_DOCUMENT_JSON : FULL_DOCUMENT_JSON;
  }

  static String getDocumentKey(final boolean simplified) {
    return simplified
        ? "{\"_id\": \"5f15aab12435743f9bd126a4\"}"
        : "{\"_id\": {\"$oid\": \"5f15aab12435743f9bd126a4\"}}";
  }

  static String getUpdatedField(final boolean simplified) {
    return simplified
        ? "{\"myString\": \"some foo bla text\", \"myInt\": 42}"
        : "{\"myString\": \"some foo bla text\", \"myInt\": {\"$numberInt\": \"42\"}}";
  }

  static String getLsidId(final boolean simplified) {
    return getLsidId(simplified, false);
  }

  static String getLsidId(final boolean simplified, final boolean quoted) {
    return simplified
        ? quoted ? "\"c//SZESzTGmQ6OfR38A11A==\"" : "c//SZESzTGmQ6OfR38A11A=="
        : "{\"$binary\": {\"base64\": \"c//SZESzTGmQ6OfR38A11A==\", \"subType\": \"04\"}}";
  }

  static String getLsidUid(final boolean simplified) {
    return getLsidUid(simplified, false);
  }

  static String getLsidUid(final boolean simplified, final boolean quoted) {
    return simplified
        ? quoted ? "\"1000000000000w==\"" : "1000000000000w=="
        : "{\"$binary\": {\"base64\": \"1000000000000w==\", \"subType\": \"00\"}}";
  }

  static String generateJson(final boolean simplified) {
    return format(
        "{\"_id\": {\"_data\": \"5f15aab12435743f9bd126a4\"},"
            + " \"operationType\": \"<operation>\","
            + " \"fullDocument\": %s,"
            + " \"ns\": {\"db\": \"<database>\", \"coll\": \"<collection>\"},"
            + " \"to\": {\"db\": \"<to_database>\", \"coll\": \"<to_collection>\"},"
            + " \"documentKey\": %s,"
            + " \"updateDescription\":"
            + " {\"updatedFields\": %s,"
            + " \"removedFields\": [\"legacyUUID\"]},"
            + " \"clusterTime\": {\"$timestamp\": {\"t\": 123456789, \"i\": 42}},"
            + " \"txnNumber\": 987654321,"
            + " \"lsid\": {\"id\": %s, \"uid\": %s}"
            + "}",
        getFullDocument(simplified),
        getDocumentKey(simplified),
        getUpdatedField(simplified),
        getLsidId(simplified, true),
        getLsidUid(simplified, true));
  }
}
