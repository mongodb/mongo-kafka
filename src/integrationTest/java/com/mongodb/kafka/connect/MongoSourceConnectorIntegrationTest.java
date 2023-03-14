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

import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.concat;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createDropCollection;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createInserts;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_KEY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_KEY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils.getMBeanAttributes;
import static com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils.getMBeanDescriptionFor;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.log.LogCapture;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;
import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.source.MongoSourceTask;
import com.mongodb.kafka.connect.util.jmx.SourceTaskStatistics;

import com.fasterxml.jackson.databind.JsonNode;

public class MongoSourceConnectorIntegrationTest extends MongoKafkaTestCase {

  @BeforeEach
  void setUp() {
    assumeTrue(isReplicaSetOrSharded());
    cleanUp();
  }

  @AfterEach
  void tearDown() {
    cleanUp();
  }

  @Test
  @DisplayName("Ensure source loads data from MongoClient")
  void testSourceLoadsDataFromMongoClient() {
    assumeTrue(isGreaterThanThreeDotSix());
    addSourceConnector();

    MongoDatabase db1 = getDatabaseWithPostfix();
    MongoDatabase db2 = getDatabaseWithPostfix();
    MongoDatabase db3 = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = db1.getCollection("coll");
    MongoCollection<Document> coll2 = db2.getCollection("coll");
    MongoCollection<Document> coll3 = db3.getCollection("coll");
    MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

    insertMany(rangeClosed(1, 50), coll1, coll2);

    db1.drop();
    sleep();
    insertMany(rangeClosed(51, 60), coll2, coll4);
    insertMany(rangeClosed(1, 70), coll3);

    assertAll(
        () ->
            assertProduced(
                concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
        () -> assertProduced(createInserts(1, 60), coll2),
        () -> assertProduced(createInserts(1, 70), coll3),
        () -> assertProduced(createInserts(51, 60), coll4));
    assertMetrics();

    Map<String, Map<String, Long>> mBeansMap = getMBeanAttributes("com.mongodb.kafka.connect:*");
    Map<String, Long> empty =
        mBeansMap.remove(
            "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-copy-existing-0");
    for (Map.Entry<String, Long> entry : empty.entrySet()) {
      assertEquals(0, entry.getValue(), entry.getKey());
    }
    for (Map<String, Long> attrs : mBeansMap.values()) {
      assertMBeanAttributesRecorded(attrs, false);
    }
  }

  private void assertMBeanAttributesRecorded(
      final Map<String, Long> attrs, final boolean skipInitiating) {
    assertNotEquals(0, attrs.get("records"));
    assertEquals(0, attrs.get("records-filtered"));
    assertNotEquals(0, attrs.get("records-acknowledged"));
    assertNotEquals(0, attrs.get("mongodb-bytes-read"));
    // skip "latest-offset-secs"
    assertNotEquals(0, attrs.get("in-task-poll"));
    if (attrs.get("in-task-poll") > 1) {
      assertNotEquals(0, attrs.get("in-connect-framework"));
    }
    if (!skipInitiating) {
      assertNotEquals(0, attrs.get("initial-commands-successful"));
    }
    assertNotEquals(0, attrs.get("getmore-commands-successful"));
    assertEquals(0, attrs.get("initial-commands-failed"));
    assertEquals(0, attrs.get("getmore-commands-failed"));
  }

  @Test
  @DisplayName("Ensure source loads data from MongoClient with copy existing data")
  void testSourceLoadsDataFromMongoClientWithCopyExisting() {
    assumeTrue(isGreaterThanThreeDotSix());
    Properties sourceProperties = new Properties();
    sourceProperties.put(
        MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());

    MongoDatabase db1 = getDatabaseWithPostfix();
    MongoDatabase db2 = getDatabaseWithPostfix();
    MongoDatabase db3 = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = db1.getCollection("coll");
    MongoCollection<Document> coll2 = db2.getCollection("coll");
    MongoCollection<Document> coll3 = db3.getCollection("coll");
    MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

    insertMany(rangeClosed(1, 50), coll1, coll2);
    addSourceConnector(sourceProperties);
    assertAll(
        () -> assertProduced(createInserts(1, 50), coll1),
        () -> assertProduced(createInserts(1, 50), coll1));

    db1.drop();
    insertMany(rangeClosed(51, 60), coll2, coll4);
    insertMany(rangeClosed(1, 70), coll3);

    assertAll(
        () ->
            assertProduced(
                concat(createInserts(1, 50), singletonList(createDropCollection())), coll1),
        () -> assertProduced(createInserts(1, 60), coll2),
        () -> assertProduced(createInserts(1, 70), coll3),
        () -> assertProduced(createInserts(51, 60), coll4));
    assertMetrics();

    Map<String, Map<String, Long>> mBeansMap = getMBeanAttributes("com.mongodb.kafka.connect:*");
    assertMBeanAttributesRecorded(
        mBeansMap.get(
            "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-copy-existing-0"),
        false);
    assertMBeanAttributesRecorded(
        mBeansMap.get(
            "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-change-stream-0"),
        true);
    assertMBeanAttributesRecorded(
        mBeansMap.get(
            "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-0"),
        false);
    assertEquals(3, mBeansMap.size());
  }

  @Test
  @DisplayName("Ensure source loads data from collection with copy existing data - outputting json")
  void testSourceLoadsDataFromCollectionCopyExistingJson() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoCollection<Document> coll = getAndCreateCollection();

    insertMany(rangeClosed(1, 50), coll);

    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
    sourceProperties.put(
        MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
    sourceProperties.put(
        MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
    addSourceConnector(sourceProperties);

    insertMany(rangeClosed(51, 100), coll);
    assertProduced(createInserts(1, 100), coll);
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure source loads data from collection with copy existing data - outputting bson")
  void testSourceLoadsDataFromCollectionCopyExistingBson() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoCollection<Document> coll = getAndCreateCollection();

    insertMany(rangeClosed(1, 50), coll);

    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
    sourceProperties.put(
        MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
    sourceProperties.put(
        MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
    sourceProperties.put(OUTPUT_FORMAT_KEY_CONFIG, OutputFormat.BSON.name());
    sourceProperties.put(MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.BSON.name());
    sourceProperties.put("key.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");
    sourceProperties.put(
        "value.converter", "org.apache.kafka.connect.converters.ByteArrayConverter");

    addSourceConnector(sourceProperties);

    insertMany(rangeClosed(51, 100), coll);
    assertProduced(createInserts(1, 100), coll, OutputFormat.BSON);
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure source loads data from collection with copy existing data by regex")
  void testSourceLoadsDataFromCollectionCopyExistingByRegex() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoDatabase db1 = getDatabaseWithPostfix();
    MongoDatabase db2 = getDatabaseWithPostfix();
    MongoDatabase db3 = getDatabaseWithPostfix();
    MongoCollection<Document> coll1 = db1.getCollection("coll1");
    MongoCollection<Document> coll21 = db2.getCollection("coll1");
    MongoCollection<Document> coll22 = db2.getCollection("coll2");
    MongoCollection<Document> coll23 = db2.getCollection("coll3");
    MongoCollection<Document> coll3 = db3.getCollection("coll1");

    insertMany(rangeClosed(1, 50), coll1);
    insertMany(rangeClosed(1, 50), coll21);
    insertMany(rangeClosed(1, 50), coll22);
    insertMany(rangeClosed(1, 50), coll23);
    insertMany(rangeClosed(1, 50), coll3);

    Properties sourceProperties = new Properties();
    sourceProperties.put(
        MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
    String namespaceRegex =
        String.format("(%s\\.coll1|%s\\.coll(1|3))", db1.getName(), db2.getName());
    sourceProperties.put(
        MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG, namespaceRegex);

    addSourceConnector(sourceProperties);

    insertMany(rangeClosed(51, 100), coll1);
    insertMany(rangeClosed(51, 100), coll21);
    insertMany(rangeClosed(51, 100), coll22);
    insertMany(rangeClosed(51, 100), coll23);
    insertMany(rangeClosed(51, 100), coll3);
    assertProduced(createInserts(1, 100), coll1);
    assertProduced(createInserts(1, 100), coll21);
    assertProduced(createInserts(51, 100), coll22);
    assertProduced(createInserts(1, 100), coll23);
    assertProduced(createInserts(51, 100), coll3);
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure Schema Key and Value output")
  void testSchemaKeyAndValueOutput() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoCollection<Document> coll = getDatabaseWithPostfix().getCollection("coll");
    insertMany(rangeClosed(1, 10), coll);

    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
    sourceProperties.put(
        MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());

    sourceProperties.put(OUTPUT_FORMAT_KEY_CONFIG, OutputFormat.SCHEMA.name());
    sourceProperties.put(
        OUTPUT_SCHEMA_KEY_CONFIG,
        "{\"type\" : \"record\", \"name\" : \"key\","
            + "\"fields\" : [{\"name\": \"key\", \"type\": [\"int\",  \"null\"]}]}");
    sourceProperties.put(OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.SCHEMA.name());
    sourceProperties.put(
        OUTPUT_SCHEMA_VALUE_CONFIG,
        "{\"type\" : \"record\", \"name\" : \"fullDocument\","
            + "\"fields\" : [{\"name\": \"value\", \"type\": [\"int\",  \"null\"]}]}");
    sourceProperties.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    sourceProperties.put("key.converter.schemas.enable", "false");
    sourceProperties.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    sourceProperties.put("value.converter.schemas.enable", "false");
    sourceProperties.put(
        PIPELINE_CONFIG,
        "[{\"$addFields\": {\"key\": \"$fullDocument._id\", "
            + "\"value\": \"$fullDocument._id\"}}]");
    addSourceConnector(sourceProperties);

    List<ConsumerRecord<Integer, Integer>> expected =
        rangeClosed(1, 10)
            .boxed()
            .map(i -> new ConsumerRecord<>(coll.getNamespace().getFullName(), 0, 0, i, i))
            .collect(toList());

    Deserializer<Integer> deserializer = new KeyValueDeserializer();
    List<ConsumerRecord<Integer, Integer>> produced =
        getProduced(
            coll.getNamespace().getFullName(),
            deserializer,
            deserializer,
            cr ->
                new ConsumerRecord<>(coll.getNamespace().getFullName(), 0, 0, cr.key(), cr.value()),
            expected,
            10);

    assertIterableEquals(
        produced.stream().map(ConsumerRecord::key).collect(toList()),
        expected.stream().map(ConsumerRecord::key).collect(toList()));
    assertIterableEquals(
        produced.stream().map(ConsumerRecord::value).collect(toList()),
        expected.stream().map(ConsumerRecord::value).collect(toList()));
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure Source uses heartbeats for creating offsets")
  void testSourceUsesHeartbeatsForOffsets() {
    assumeTrue(isGreaterThanFourDotZero());
    try (LogCapture logCapture = new LogCapture(Logger.getLogger(MongoSourceTask.class))) {
      MongoCollection<Document> coll = getAndCreateCollection();
      MongoCollection<Document> altColl = getAndCreateCollection();

      String heartbeatTopic = "__HEARTBEATS";

      Properties sourceProperties = new Properties();
      sourceProperties.put(
          MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
      sourceProperties.put(
          MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
      sourceProperties.put(MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
      sourceProperties.put(MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG, heartbeatTopic);

      addSourceConnector(sourceProperties);

      insertMany(rangeClosed(1, 50), coll);
      insertMany(rangeClosed(1, 50), altColl);
      getProducedStrings(heartbeatTopic, 1);

      assertMetrics();

      stopStartSourceConnector(sourceProperties);

      boolean resumedFromHeartbeat =
          logCapture.getEvents().stream()
              .map(e -> e.getMessage().toString())
              .anyMatch(e -> e.startsWith("Resume token from heartbeat"));

      assertTrue(resumedFromHeartbeat);
    }
  }

  @Test
  @DisplayName("Ensure Source heartbeats have a valid schema")
  void testSourceHeartbeatsHaveValidSchema() {
    assumeTrue(isGreaterThanFourDotZero());

    MongoCollection<Document> coll = getAndCreateCollection();

    String heartbeatTopic = "__HEARTBEAT_SCHEMA";

    Properties sourceProperties = new Properties();
    sourceProperties.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
    sourceProperties.put(
        MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
    sourceProperties.put(MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
    sourceProperties.put(MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG, heartbeatTopic);
    sourceProperties.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    sourceProperties.put("key.converter.schemas.enable", "false");
    sourceProperties.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    sourceProperties.put("value.converter.schemas.enable", "false");

    addSourceConnector(sourceProperties);
    BsonDocument heartbeat = getHeartbeat(heartbeatTopic);

    assertTrue(heartbeat.get("key").isDocument());
    assertTrue(heartbeat.get("value").isNull());
    assertMetrics();
  }

  @Test
  @DisplayName("Ensure Source provides friendly error messages for invalid pipelines")
  void testSourceHasFriendlyErrorMessagesForInvalidPipelines() {
    assumeTrue(isGreaterThanFourDotZero());
    try (LogCapture logCapture = new LogCapture(Logger.getLogger(MongoSourceTask.class))) {
      MongoCollection<Document> coll = getAndCreateCollection();

      Properties sourceProperties = new Properties();
      sourceProperties.put(
          MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
      sourceProperties.put(
          MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
      sourceProperties.put(PIPELINE_CONFIG, "[{'$group': {_id: 1 }}]");

      addSourceConnector(sourceProperties);

      boolean containsIllegalChangeStreamOperation =
          logCapture.getEvents().stream()
              .map(e -> e.getMessage().toString())
              .anyMatch(e -> e.startsWith("Illegal $changeStream operation"));

      assertTrue(containsIllegalChangeStreamOperation);
    }
  }

  private void assertMetrics() {
    Set<String> names = SourceTaskStatistics.DESCRIPTIONS.keySet();

    String mBeanName =
        "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-0";
    Map<String, Map<String, Long>> mBeansMap = getMBeanAttributes(mBeanName);
    assertTrue(mBeansMap.size() > 0);
    for (Map.Entry<String, Map<String, Long>> entry : mBeansMap.entrySet()) {
      assertEquals(
          names, entry.getValue().keySet(), "Mismatched MBean attributes for " + entry.getKey());
      entry.getValue().keySet().forEach(n -> assertNotNull(getMBeanDescriptionFor(mBeanName, n)));
    }
    Set<String> initialNames = new HashSet<>();
    new SourceTaskStatistics("name").emit(v -> initialNames.add(v.getName()));
    assertEquals(names, initialNames, "Attributes must not be added after construction");
  }

  public static class KeyValueDeserializer implements Deserializer<Integer> {

    static final JsonDeserializer JSON_DESERIALIZER = new JsonDeserializer();

    @Override
    public Integer deserialize(final String topic, final byte[] data) {
      JsonNode node = JSON_DESERIALIZER.deserialize(topic, data);
      Iterable<String> iterable = node::fieldNames;
      List<String> fieldNames =
          StreamSupport.stream(iterable.spliterator(), false).collect(toList());
      if (fieldNames.contains("key")) {
        return node.get("key").asInt();
      } else if (fieldNames.contains("value")) {
        return node.get("value").asInt();
      }
      return -1;
    }
  }
}
