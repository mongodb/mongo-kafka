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
import static com.mongodb.kafka.connect.source.schema.SchemaUtils.assertSchemaAndValueEquals;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
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
      "{\"_id\": {\"$oid\": \"5f15aab12435743f9bd126a4\"},"
          + " \"myString\": \"some foo bla text\","
          + " \"myInt\": {\"$numberInt\": \"42\"},"
          + " \"myDouble\": {\"$numberDouble\": \"20.21\"},"
          + " \"mySubDoc\": {\"A\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
          + " \"B\": {\"$date\": {\"$numberLong\": \"1577863627000\"}},"
          + " \"C\": {\"$numberDecimal\": \"12345.6789\"}},"
          + " \"myArray\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}],"
          + " \"myBytes\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}},"
          + " \"myDate\": {\"$date\": {\"$numberLong\": \"1234567890\"}},"
          + " \"myDecimal\": {\"$numberDecimal\": \"12345.6789\"}"
          + "}";

  private static final String SIMPLIFIED_FULL_DOCUMENT_JSON =
      "{\"_id\": \"5f15aab12435743f9bd126a4\", "
          + "\"myString\": \"some foo bla text\", "
          + "\"myInt\": 42, "
          + "\"myDouble\": 20.21, "
          + "\"mySubDoc\": {\"A\": \"S2Fma2Egcm9ja3Mh\", \"B\": \"2020-01-01T07:27:07Z\", \"C\": \"12345.6789\"}, "
          + "\"myArray\": [1, 2, 3], "
          + "\"myBytes\": \"S2Fma2Egcm9ja3Mh\", "
          + "\"myDate\": \"1970-01-15T06:56:07.89Z\", "
          + "\"myDecimal\": \"12345.6789\"}";

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
    SchemaAndValue expectedKey =
        new SchemaAndValue(
            DEFAULT_KEY_SCHEMA,
            new Struct(DEFAULT_KEY_SCHEMA).put("_id", "{\"_data\": \"5f15aab12435743f9bd126a4\"}"));

    SchemaAndValue expectedValue =
        new SchemaAndValue(DEFAULT_VALUE_SCHEMA, generateExpectedValue(true));
    SchemaAndValue expectedExtendedValue =
        new SchemaAndValue(DEFAULT_VALUE_SCHEMA, generateExpectedValue(false));

    AvroSchemaAndValueProducer keyProducer =
        new AvroSchemaAndValueProducer(DEFAULT_AVRO_KEY_SCHEMA, SIMPLE_JSON_WRITER_SETTINGS);

    AvroSchemaAndValueProducer valueProducer =
        new AvroSchemaAndValueProducer(DEFAULT_AVRO_VALUE_SCHEMA, SIMPLE_JSON_WRITER_SETTINGS);

    AvroSchemaAndValueProducer extendedJsonValueProducer =
        new AvroSchemaAndValueProducer(DEFAULT_AVRO_VALUE_SCHEMA, EXTENDED_JSON_WRITER_SETTINGS);

    assertSchemaAndValueEquals(expectedKey, keyProducer.get(CHANGE_STREAM_DOCUMENT));
    assertSchemaAndValueEquals(expectedValue, valueProducer.get(CHANGE_STREAM_DOCUMENT));
    assertSchemaAndValueEquals(
        expectedExtendedValue, extendedJsonValueProducer.get(CHANGE_STREAM_DOCUMENT));
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
        () -> assertEquals(708, ((byte[]) actual.value()).length),
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
