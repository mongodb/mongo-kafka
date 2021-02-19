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
package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_TOLERANCE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_INFER_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.log.LogCapture;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.source.MongoSourceConfig.ErrorTolerance;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;

public class FullDocumentRoundTripIntegrationTest extends MongoKafkaTestCase {

  @BeforeEach
  void setUp() {
    assumeTrue(isReplicaSetOrSharded());
    assumeTrue(isGreaterThanFourDotTwo());
  }

  @AfterEach
  void tearDown() {
    getMongoClient()
        .listDatabaseNames()
        .into(new ArrayList<>())
        .forEach(
            i -> {
              if (i.startsWith(getDatabaseName())) {
                getMongoClient().getDatabase(i).drop();
              }
            });
  }

  private static final String FULL_DOCUMENT_JSON =
      "{\"_id\": %s,"
          + " \"myObjectId\": {\"$oid\": \"5f15aab12435743f9bd126a4\"},"
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
      "{\"_id\": %s, "
          + "\"myObjectId\": \"5f15aab12435743f9bd126a4\", "
          + "\"myString\": \"some foo bla text\", "
          + "\"myInt\": 42, "
          + "\"myDouble\": 20.21, "
          + "\"mySubDoc\": {\"A\": \"S2Fma2Egcm9ja3Mh\", \"B\": \"2020-01-01T07:27:07Z\", \"C\": \"12345.6789\"}, "
          + "\"myArray\": [1, 2, 3], "
          + "\"myBytes\": \"S2Fma2Egcm9ja3Mh\", "
          + "\"myDate\": \"1970-01-15T06:56:07.89Z\", "
          + "\"myDecimal\": \"12345.6789\"}";

  private static final String AVRO_VALUE_SCHEMA =
      "{\"type\" : \"record\", \"name\" : \"fullDocument\","
          + "\"fields\" : ["
          + "  {\"name\": \"_id\", \"type\": \"int\"}, "
          + "  {\"name\": \"myObjectId\", \"type\": \"string\"}, "
          + "  {\"name\": \"myString\", \"type\": \"string\"}, "
          + "  {\"name\": \"myInt\", \"type\": \"int\"}, "
          + "  {\"name\": \"myDouble\", \"type\": \"double\"}, "
          + "  {\"name\": \"mySubDoc\", \"type\": {"
          + "      \"type\" : \"record\", \"name\" : \"mySubDocWithStringValues\","
          + "      \"fields\" : ["
          + "       {\"name\" : \"A\", \"type\" : \"string\"}, "
          + "       {\"name\" : \"B\", \"type\" : \"string\"}, "
          + "       {\"name\" : \"C\", \"type\" : \"string\"}"
          + "      ]"
          + "    }"
          + "  }, "
          + "  {\"name\": \"myArray\", \"type\": {\"type\" : \"array\", \"items\" : \"int\"}}, "
          + "  {\"name\": \"myBytes\", \"type\": \"string\"}, "
          + "  {\"name\": \"myDate\", \"type\": \"string\"}, "
          + "  {\"name\": \"myDecimal\", \"type\": \"string\"}"
          + "]}";

  private static final Properties EMPTY_PROPERTIES = new Properties();

  @Test
  @DisplayName("Ensure collection round trip default settings")
  void testRoundTripDefault() {
    assertRoundTrip(
        IntStream.range(1, 100)
            .mapToObj(i -> BsonDocument.parse(format(FULL_DOCUMENT_JSON, i)))
            .collect(toList()));
  }

  @Test
  @DisplayName("Ensure collection round trip simple json format settings")
  void testRoundTripSimpleJsonFormat() {
    Properties sourceProperties = new Properties();
    sourceProperties.put(
        OUTPUT_JSON_FORMATTER_CONFIG,
        "com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson");
    assertRoundTrip(
        IntStream.range(1, 100)
            .mapToObj(i -> BsonDocument.parse(format(FULL_DOCUMENT_JSON, i)))
            .collect(toList()),
        IntStream.range(1, 100)
            .mapToObj(i -> BsonDocument.parse(format(SIMPLIFIED_FULL_DOCUMENT_JSON, i)))
            .collect(toList()),
        sourceProperties);
  }

  @Test
  @DisplayName("Ensure collection round trip using BSON")
  void testRoundTripBSON() {
    Properties sourceProperties = new Properties();
    sourceProperties.put(OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.BSON.name());
    sourceProperties.put("value.converter", ByteArrayConverter.class.getName());

    Properties sinkProperties = new Properties();
    sinkProperties.put("value.converter", ByteArrayConverter.class.getName());

    assertRoundTrip(
        IntStream.range(1, 100)
            .mapToObj(i -> BsonDocument.parse(format(FULL_DOCUMENT_JSON, i)))
            .collect(toList()),
        sourceProperties,
        sinkProperties);
  }

  @Test
  @DisplayName("Ensure collection round trip using Avro Schema")
  void testRoundTripSchema() {
    Properties sourceProperties = new Properties();
    sourceProperties.put(
        OUTPUT_JSON_FORMATTER_CONFIG,
        "com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson");
    sourceProperties.put(OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.SCHEMA.name());
    sourceProperties.put(OUTPUT_SCHEMA_VALUE_CONFIG, AVRO_VALUE_SCHEMA);
    sourceProperties.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    sourceProperties.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());

    Properties sinkProperties = new Properties();
    sinkProperties.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    sinkProperties.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());

    assertRoundTrip(
        IntStream.range(1, 100)
            .mapToObj(i -> BsonDocument.parse(format(FULL_DOCUMENT_JSON, i)))
            .collect(toList()),
        IntStream.range(1, 100)
            .mapToObj(i -> BsonDocument.parse(format(SIMPLIFIED_FULL_DOCUMENT_JSON, i)))
            .collect(toList()),
        sourceProperties,
        sinkProperties);
  }

  @Test
  @DisplayName("Ensure collection round trip inferring schema value")
  void testRoundTripInferSchemaValue() {
    try (LogCapture logCapture =
        new LogCapture(
            Logger.getLogger("io.confluent.rest.exceptions.DebuggableExceptionMapper"))) {
      Properties sourceProperties = new Properties();
      sourceProperties.put(
          OUTPUT_JSON_FORMATTER_CONFIG,
          "com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson");
      sourceProperties.put(OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.SCHEMA.name());
      sourceProperties.put(OUTPUT_SCHEMA_INFER_VALUE_CONFIG, "true");
      sourceProperties.put(PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
      sourceProperties.put("value.converter", "io.confluent.connect.avro.AvroConverter");
      sourceProperties.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());
      sourceProperties.put(ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
      sourceProperties.put(ERRORS_LOG_ENABLE_CONFIG, "true");

      Properties sinkProperties = new Properties();
      sinkProperties.put("value.converter", "io.confluent.connect.avro.AvroConverter");
      sinkProperties.put("value.converter.schema.registry.url", KAFKA.schemaRegistryUrl());

      List<BsonDocument> originals =
          Stream.of(
                  "{_id: 1, a: 1, b: 1}",
                  "{b: 1, _id: 2, a: 1}", // Different field order
                  "{_id: 3, b: 1, c: 1, d: 1}", // Missing a field and added two new fields
                  "{_id: 4, E: 1, f: 1, g: 1, h: {h1: 2, h2: '2'}}", // All new fields
                  "{_id: 5, h: {h2: '3', h1: 2, h4: [1]}}", // Nested field order
                  "{_id: 10, h: ['1']}", // Invalid schema ignored due to errors.tolerance
                  "{_id: 6, g: 3, a: 2, h: {h1: 2, h2: '2'}}" // Different field order
                  )
              .map(BsonDocument::parse)
              .collect(toList());

      List<BsonDocument> expected =
          originals.stream().filter(d -> d.getInt32("_id").getValue() != 10).collect(toList());

      assertRoundTrip(originals, expected, sourceProperties, sinkProperties);

      assertTrue(
          logCapture.getEvents().stream()
              .filter(e -> e.getThrowableInformation() != null)
              .anyMatch(
                  e ->
                      e.getThrowableInformation()
                          .getThrowable()
                          .getMessage()
                          .contains(
                              "Schema being registered is incompatible with an earlier schema")));
    }
  }

  void assertRoundTrip(final List<BsonDocument> originals) {
    assertRoundTrip(originals, originals, EMPTY_PROPERTIES);
  }

  void assertRoundTrip(
      final List<BsonDocument> originals,
      final Properties sourcePropertyOverrides,
      final Properties sinkPropertyOverrides) {
    assertRoundTrip(originals, originals, sourcePropertyOverrides, sinkPropertyOverrides);
  }

  void assertRoundTrip(
      final List<BsonDocument> originals,
      final List<BsonDocument> expected,
      final Properties sourcePropertyOverrides) {
    assertRoundTrip(originals, expected, sourcePropertyOverrides, EMPTY_PROPERTIES);
  }

  void assertRoundTrip(
      final List<BsonDocument> originals,
      final List<BsonDocument> expected,
      final Properties sourcePropertyOverrides,
      final Properties sinkPropertyOverrides) {

    MongoDatabase database = getDatabaseWithPostfix();
    MongoCollection<BsonDocument> source = database.getCollection("source", BsonDocument.class);
    MongoCollection<BsonDocument> destination =
        database.getCollection("destination", BsonDocument.class);

    Properties sourceProperties = new Properties();
    sourceProperties.put(DATABASE_CONFIG, source.getNamespace().getDatabaseName());
    sourceProperties.put(COLLECTION_CONFIG, source.getNamespace().getCollectionName());
    sourceProperties.put(TOPIC_PREFIX_CONFIG, "copy");
    sourceProperties.put(COPY_EXISTING_CONFIG, "true");
    sourceProperties.put(PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
    sourceProperties.putAll(sourcePropertyOverrides);
    addSourceConnector(sourceProperties);

    Properties sinkProperties = createSinkProperties();
    sinkProperties.put(
        "topics",
        format(
            "copy.%s.%s",
            source.getNamespace().getDatabaseName(), source.getNamespace().getCollectionName()));
    sinkProperties.put(DATABASE_CONFIG, destination.getNamespace().getDatabaseName());
    sinkProperties.put(COLLECTION_CONFIG, destination.getNamespace().getCollectionName());
    sinkProperties.put("key.converter", StringConverter.class.getName());
    sinkProperties.put("value.converter", StringConverter.class.getName());
    sinkProperties.putAll(sinkPropertyOverrides);

    addSinkConnector(sinkProperties);
    source.insertMany(originals);

    assertCollection(expected, destination);
  }
}
