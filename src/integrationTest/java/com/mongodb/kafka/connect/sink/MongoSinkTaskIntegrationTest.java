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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
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

  private static final String TOPIC = "topic-test";

  @Test
  @DisplayName("Ensure sink processes data from Kafka")
  void testSinkProcessesSinkRecords() {
    try (AutoCloseableSinkTask task = createSinkTask()) {
      Map<String, String> cfg = createSettings();
      task.start(cfg);

      List<Document> documents = createDocuments(rangeClosed(1, 10));
      List<SinkRecord> sinkRecords = createRecords(documents);

      task.put(sinkRecords);
      assertCollection(documents, getCollection());
    }
  }

  @Test
  @DisplayName("Ensure sink can handle Tombstone null events")
  void testSinkCanHandleTombstoneNullEvents() {
    try (AutoCloseableSinkTask task = createSinkTask()) {

      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
          "com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler");
      task.start(cfg);

      List<Document> documents =
          rangeClosed(1, 10)
              .mapToObj(i -> Document.parse(format("{_id: %s, a: 1, b: 2}", i)))
              .collect(toList());

      List<SinkRecord> sinkRecords =
          createRecords(
              documents.stream(),
              Document::toJson,
              d -> format("{op: 'c', after: '%s'}", d.toJson()),
              d -> d.get("_id", 0));

      sinkRecords.add(
          5,
          new SinkRecord(
              TOPIC, 0, Schema.STRING_SCHEMA, "{id: 0}", Schema.STRING_SCHEMA, null, 55));

      task.put(sinkRecords);
      assertCollection(documents, getCollection());
    }
  }

  @Test
  @DisplayName("Ensure sink can handle poison pill invalid key")
  void testSinkCanHandleInvalidKeyWhenErrorToleranceIsAll() {
    try (AutoCloseableSinkTask task = createSinkTask()) {

      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialKeyStrategy");
      cfg.put(
          MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG,
          "AllowList");
      cfg.put(MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG, "a,b");
      cfg.put(
          MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG,
          "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy");
      cfg.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "All");

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

      List<SinkRecord> sinkRecords = createRecords(documents);

      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> !d.containsKey("_id")).collect(toList()),
          getCollection().find().projection(excludeId()).into(new ArrayList<>()));

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

      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG,
          "com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy");
      cfg.put(
          MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_TYPE_CONFIG,
          "AllowList");
      cfg.put(
          MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_LIST_CONFIG, "a,b");
      cfg.put(
          MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG,
          "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy");
      cfg.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "All");
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
          createRecords(documents.stream(), Document::toJson, Document::toJson, d -> d.get("c", 0));
      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> !d.containsKey("_id")).collect(toList()),
          getCollection().find().projection(excludeId()).into(new ArrayList<>()));

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

      Map<String, String> cfg = createSettings();
      cfg.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "all");
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
          createRecords(
              documents.stream(),
              Document::toJson,
              d -> d.get("_id", 0) != 4 ? d.toJson() : "a",
              d -> d.get("c", 0));
      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> d.get("_id", 0) != 4).collect(toList()),
          getCollection().find().into(new ArrayList<>()));

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

      Map<String, String> cfg = createSettings();
      cfg.put(
          MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
          "com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler");
      cfg.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, "all");
      task.start(cfg);

      List<Document> documents =
          rangeClosed(1, 11)
              .mapToObj(i -> Document.parse(format("{_id: %s, a: 1, b: 2}", i)))
              .collect(toList());

      List<SinkRecord> sinkRecords =
          createRecords(
              documents.stream(),
              Document::toJson,
              d ->
                  d.get("_id", 0) != 4 ? format("{op: 'c', after: '%s'}", d.toJson()) : "{op: 'c'}",
              d -> d.get("_id", 0));

      task.put(sinkRecords);

      assertIterableEquals(
          documents.stream().filter(d -> d.getInteger("_id", 0) != 4).collect(toList()),
          getCollection().find().into(new ArrayList<>()));

      task.stop();

      cfg.remove(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG);
      task.start(cfg);

      DataException e = assertThrows(DataException.class, () -> task.put(sinkRecords));
      assertTrue(e.getMessage().contains("Insert document missing `after` field"));
    }
  }

  @Test
  @DisplayName("Ensure sink regex timeseries errors if cannot create")
  void testSinkRegexTimeseriesCannotCreate() {
    Map<String, String> cfg = createSettings();
    cfg.remove(MongoSinkConfig.TOPICS_CONFIG);
    cfg.put(MongoSinkConfig.TOPICS_REGEX_CONFIG, "topic-(.*)");
    cfg.put(MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG, "ts");
    cfg.put(MongoSinkTopicConfig.TIMESERIES_METAFIELD_CONFIG, "meta");

    getCollection().insertOne(new Document());
    try (AutoCloseableSinkTask task = createSinkTask()) {
      task.start(cfg);

      List<Document> documents =
          rangeClosed(1, 11)
              .mapToObj(
                  i -> {
                    Document doc = new Document("_id", i);
                    doc.put("ts", new Date());
                    doc.put("meta", "meta");
                    return doc;
                  })
              .collect(toList());
      List<SinkRecord> sinkRecords = createRecords(documents);
      assertThrows(ConfigException.class, () -> task.put(sinkRecords));
    }
  }

  @Test
  @DisplayName("Ensure sink regex timeseries errors missing timefield create")
  void testSinkRegexTimeseriesMissingTimefield() {
    assumeTrue(isGreaterThanFourDotFour());
    Map<String, String> cfg = createSettings();
    cfg.remove(MongoSinkConfig.TOPICS_CONFIG);
    cfg.put(MongoSinkConfig.TOPICS_REGEX_CONFIG, "topic-(.*)");
    cfg.put(MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG, "ts");
    cfg.put(MongoSinkTopicConfig.TIMESERIES_METAFIELD_CONFIG, "meta");

    try (AutoCloseableSinkTask task = createSinkTask()) {
      task.start(cfg);

      List<Document> documents =
          rangeClosed(1, 11)
              .mapToObj(
                  i -> {
                    Document doc = new Document("_id", i);
                    if (i != 4) {
                      doc.put("ts", new Date());
                    }
                    doc.put("meta", "meta");
                    return doc;
                  })
              .collect(toList());
      List<SinkRecord> sinkRecords = createRecords(documents);
      assertThrows(DataException.class, () -> task.put(sinkRecords));
    }
  }

  @Test
  @DisplayName("Ensure sink regex timeseries works as expected")
  void testSinkRegexTimeseriesWorks() {
    assumeTrue(isGreaterThanFourDotFour());
    Map<String, String> cfg = createSettings();
    cfg.remove(MongoSinkConfig.TOPICS_CONFIG);
    cfg.put(MongoSinkConfig.TOPICS_REGEX_CONFIG, "topic-(.*)");
    cfg.put(MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG, "ts");
    cfg.put(MongoSinkTopicConfig.TIMESERIES_METAFIELD_CONFIG, "meta");

    try (AutoCloseableSinkTask task = createSinkTask()) {
      task.start(cfg);

      List<Document> documents =
          rangeClosed(1, 11)
              .mapToObj(
                  i -> {
                    Document doc = new Document("_id", i);
                    doc.put("ts", new Date());
                    doc.put("meta", i);
                    return doc;
                  })
              .collect(toList());
      List<SinkRecord> sinkRecords = createRecords(documents);
      task.put(sinkRecords);
      assertEquals(getCollection().countDocuments(), 11);
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

  private Map<String, String> createSettings() {
    return new HashMap<String, String>() {
      {
        put(MongoSinkConfig.TOPICS_CONFIG, TOPIC);
        put(MongoSinkTopicConfig.DATABASE_CONFIG, getCollection().getNamespace().getDatabaseName());
        put(
            MongoSinkTopicConfig.COLLECTION_CONFIG,
            getCollection().getNamespace().getCollectionName());
      }
    };
  }

  static List<SinkRecord> createRecords(final List<Document> documents) {
    return createRecords(
        documents.stream(), Document::toJson, Document::toJson, d -> d.get("_id", 0));
  }

  static <I> List<SinkRecord> createRecords(
      final Stream<I> stream,
      final Function<I, String> keySupplier,
      final Function<I, String> valueSupplier,
      final Function<I, Number> kafkaOffsetSupplier) {
    return stream
        .map(
            i ->
                new SinkRecord(
                    TOPIC,
                    0,
                    Schema.STRING_SCHEMA,
                    keySupplier.apply(i),
                    Schema.STRING_SCHEMA,
                    valueSupplier.apply(i),
                    kafkaOffsetSupplier.apply(i).longValue()))
        .collect(toList());
  }
}
