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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.client.model.Projections.excludeId;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;

@RunWith(JUnitPlatform.class)
public class MongoSinkTaskIntegrationTest extends MongoKafkaTestCase {

  @BeforeEach
  void setUp() {
    cleanUp();
  }

  @AfterEach
  void tearDown() {
    cleanUp();
  }

  @Test
  @DisplayName("Ensure sink processes data from Kafka")
  void testSinkProcessesSinkRecords() {
    try (AutoCloseableSinkTask task = createSinkTask()) {

      MongoCollection<Document> collection = getCollection();
      String topic = "topic";
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSinkConfig.TOPICS_CONFIG, topic);
              put(
                  MongoSinkTopicConfig.DATABASE_CONFIG,
                  collection.getNamespace().getDatabaseName());
              put(
                  MongoSinkTopicConfig.COLLECTION_CONFIG,
                  collection.getNamespace().getCollectionName());
            }
          };
      task.start(cfg);

      List<Document> documents = createDocuments(rangeClosed(1, 10));

      List<SinkRecord> sinkRecords =
          documents.stream()
              .map(
                  d ->
                      new SinkRecord(
                          topic,
                          0,
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          d.get("_id", 0)))
              .collect(toList());

      task.put(sinkRecords);

      assertCollection(documents, collection);
    }
  }

  @Test
  @DisplayName("Ensure sink can handle poison pill invalid key")
  void testSinkCanHandleInvalidKeyWhenErrorToleranceIsAll() {
    try (AutoCloseableSinkTask task = createSinkTask()) {

      MongoCollection<Document> collection = getCollection();
      String topic = "topic";
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSinkConfig.TOPICS_CONFIG, topic);
              put(
                  MongoSinkTopicConfig.DATABASE_CONFIG,
                  collection.getNamespace().getDatabaseName());
              put(
                  MongoSinkTopicConfig.COLLECTION_CONFIG,
                  collection.getNamespace().getCollectionName());
              put(
                  MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG,
                  "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialKeyStrategy");
              put(
                  MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG,
                  "AllowList");
              put(
                  MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG,
                  "a,b");
              put(
                  MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG,
                  "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy");
              put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "All");
            }
          };
      task.start(cfg);

      List<Document> documents =
          createDocuments(rangeClosed(1, 11)).stream()
              .peek(
                  d -> {
                    int id = d.get("_id", 0);
                    d.put("a", format("a%s", id));
                    d.put("b", "b");
                    d.put("c", id);
                    if (id != 4) {
                      d.remove("_id");
                    }
                  })
              .collect(toList());

      List<SinkRecord> sinkRecords =
          documents.stream()
              .map(
                  d ->
                      new SinkRecord(
                          topic,
                          0,
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          d.get("c", 0)))
              .collect(toList());

      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> !d.containsKey("_id")).collect(toList()),
          collection.find().projection(excludeId()).into(new ArrayList<>()));

      task.stop();

      cfg.remove(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG);
      task.start(cfg);

      DataException e = assertThrows(DataException.class, () -> task.put(sinkRecords));
      assertTrue(e.getMessage().contains("Could not build the WriteModel"));
      assertTrue(
          e.getMessage()
              .contains(
                  "If you are including an existing `_id` value in the business "
                      + "key then ensure `document.id.strategy.overwrite.existing=true`"));
    }
  }

  @Test
  @DisplayName("Ensure sink can handle poison pill invalid value")
  void testSinkCanHandleInvalidValueWhenErrorToleranceIsAll() {
    try (AutoCloseableSinkTask task = createSinkTask()) {

      MongoCollection<Document> collection = getCollection();
      String topic = "topic";
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSinkConfig.TOPICS_CONFIG, topic);
              put(
                  MongoSinkTopicConfig.DATABASE_CONFIG,
                  collection.getNamespace().getDatabaseName());
              put(
                  MongoSinkTopicConfig.COLLECTION_CONFIG,
                  collection.getNamespace().getCollectionName());
              put(
                  MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG,
                  "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy");
              put(
                  MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_TYPE_CONFIG,
                  "AllowList");
              put(
                  MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_LIST_CONFIG,
                  "a,b");
              put(
                  MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG,
                  "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy");
              put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "All");
            }
          };
      task.start(cfg);

      List<Document> documents =
          createDocuments(rangeClosed(1, 11)).stream()
              .peek(
                  d -> {
                    int id = d.get("_id", 0);
                    d.put("a", format("a%s", id));
                    d.put("b", "b");
                    d.put("c", id);
                    if (id != 4) {
                      d.remove("_id");
                    }
                  })
              .collect(toList());

      List<SinkRecord> sinkRecords =
          documents.stream()
              .map(
                  d ->
                      new SinkRecord(
                          topic,
                          0,
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          d.get("c", 0)))
              .collect(toList());

      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> !d.containsKey("_id")).collect(toList()),
          collection.find().projection(excludeId()).into(new ArrayList<>()));

      task.stop();

      cfg.remove(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG);
      task.start(cfg);

      DataException e = assertThrows(DataException.class, () -> task.put(sinkRecords));
      assertTrue(e.getMessage().contains("Could not build the WriteModel"));
      assertTrue(
          e.getMessage()
              .contains(
                  "If you are including an existing `_id` value in the business "
                      + "key then ensure `document.id.strategy.overwrite.existing=true`"));
    }
  }

  @Test
  @DisplayName("Ensure sink can handle poison pill invalid document")
  void testSinkCanHandleInvalidDocumentWhenErrorToleranceIsAll() {
    try (AutoCloseableSinkTask task = createSinkTask()) {

      MongoCollection<Document> collection = getCollection();
      String topic = "topic";
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSinkConfig.TOPICS_CONFIG, topic);
              put(
                  MongoSinkTopicConfig.DATABASE_CONFIG,
                  collection.getNamespace().getDatabaseName());
              put(
                  MongoSinkTopicConfig.COLLECTION_CONFIG,
                  collection.getNamespace().getCollectionName());
              put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "all");
            }
          };
      task.start(cfg);

      List<Document> documents =
          createDocuments(rangeClosed(1, 11)).stream()
              .peek(
                  d -> {
                    d.put("a", format("a%s", d.get("_id", 0)));
                    d.put("b", "b");
                  })
              .collect(toList());

      List<SinkRecord> sinkRecords =
          documents.stream()
              .map(
                  d ->
                      new SinkRecord(
                          topic,
                          0,
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          Schema.STRING_SCHEMA,
                          d.get("_id", 0) != 4 ? d.toJson() : "a",
                          d.get("c", 0)))
              .collect(toList());

      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> d.get("_id", 0) != 4).collect(toList()),
          collection.find().into(new ArrayList<>()));

      task.stop();

      cfg.remove(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG);
      task.start(cfg);

      DataException e = assertThrows(DataException.class, () -> task.put(sinkRecords));
      assertTrue(e.getMessage().contains("Could not convert value `a` into a BsonDocument"));
    }
  }

  @Test
  @DisplayName("Ensure sink can handle poison pill CDC value")
  void testSinkCanHandleInvalidCDCWhenErrorToleranceIsAll() {
    try (AutoCloseableSinkTask task = createSinkTask()) {

      MongoCollection<Document> collection = getCollection();
      String topic = "topic";
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSinkConfig.TOPICS_CONFIG, topic);
              put(
                  MongoSinkTopicConfig.DATABASE_CONFIG,
                  collection.getNamespace().getDatabaseName());
              put(
                  MongoSinkTopicConfig.COLLECTION_CONFIG,
                  collection.getNamespace().getCollectionName());
              put(
                  MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                  "com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler");
              put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "all");
            }
          };
      task.start(cfg);

      List<Document> documents =
          rangeClosed(1, 11)
              .mapToObj(i -> Document.parse(format("{_id: %s, a: 1, b: 2}", i)))
              .collect(toList());

      List<SinkRecord> sinkRecords =
          documents.stream()
              .map(
                  d ->
                      new SinkRecord(
                          topic,
                          0,
                          Schema.STRING_SCHEMA,
                          d.toJson(),
                          Schema.STRING_SCHEMA,
                          Document.parse(
                              d.get("_id", 0) != 4
                                  ? format("{op: 'c', after: '%s'}", d.toJson())
                                  : "{op: 'c'}"),
                          d.get("_id", 0)))
              .collect(toList());

      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> d.getInteger("_id", 0) != 4).collect(toList()),
          collection.find().into(new ArrayList<>()));

      task.stop();

      cfg.remove(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG);
      task.start(cfg);

      DataException e = assertThrows(DataException.class, () -> task.put(sinkRecords));
      assertTrue(e.getMessage().contains("Insert document missing `after` field"));
    }
  }

  public AutoCloseableSinkTask createSinkTask() {
    return new AutoCloseableSinkTask(new MongoSinkTask());
  }

  static class AutoCloseableSinkTask extends SinkTask implements AutoCloseable {

    private final MongoSinkTask wrapped;

    AutoCloseableSinkTask(final MongoSinkTask wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public void close() {
      wrapped.stop();
    }

    @Override
    public String version() {
      return wrapped.version();
    }

    @Override
    public void start(final Map<String, String> overrides) {
      HashMap<String, String> props = new HashMap<>();
      props.put(MongoSinkConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
      overrides.forEach(props::put);
      wrapped.start(props);
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
      wrapped.put(records);
    }

    @Override
    public void stop() {
      wrapped.stop();
    }
  }
}
