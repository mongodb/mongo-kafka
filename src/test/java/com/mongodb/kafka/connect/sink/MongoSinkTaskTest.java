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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DELETE_ON_NULL_VALUES_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.debezium.rdbms.RdbmsHandler;
import com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy;

@SuppressWarnings("unchecked")
@RunWith(JUnitPlatform.class)
class MongoSinkTaskTest {

  @Test
  @DisplayName("test create sink record batches per topic with default topic and no batching")
  void testCreateSinkRecordBatchesPerTopicWithDefaultTopicAndNoBatching() {
    MongoSinkTask sinkTask = new MongoSinkTask();
    TopicSettingsAndResults settings = new TopicSettingsAndResults(TEST_TOPIC, 50, 0);

    sinkTask.start(createConfigMap());
    List<SinkRecord> sinkRecords = createSinkRecordList(settings);
    Map<String, RecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

    assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
    assertNotNull(batchesMap.get(TEST_TOPIC), "batch map entry for " + TEST_TOPIC + " was null");

    assertAll(
        "verify contents of created batches map",
        () ->
            assertEquals(
                1,
                batchesMap.get(TEST_TOPIC).getBufferedBatches().size(),
                "wrong number of batches in map entry for " + TEST_TOPIC),
        () ->
            assertEquals(
                settings.numRecords,
                batchesMap.get(TEST_TOPIC).getBufferedBatches().get(0).size(),
                "wrong number of records in single batch of map entry for " + TEST_TOPIC),
        () ->
            assertEquals(
                sinkRecords,
                batchesMap.get(TEST_TOPIC).getBufferedBatches().get(0),
                "sink record list mismatch in single batch of map entry for" + TEST_TOPIC));
  }

  @Test
  @DisplayName("test create sink record batches per topic with no batching")
  void testCreateSinkRecordBatchesPerTopicWithNoDefaultTopicAndNoBatching() {
    MongoSinkTask sinkTask = new MongoSinkTask();

    String topicName = "KTopic1";
    TopicSettingsAndResults settings = new TopicSettingsAndResults(topicName, 50, 0);

    sinkTask.start(createConfigMap(TOPICS_CONFIG, format("%s,%s", TEST_TOPIC, topicName)));

    List<SinkRecord> sinkRecords = createSinkRecordList(settings);
    Map<String, RecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

    assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
    assertNull(batchesMap.get(TEST_TOPIC), "batch map entry for " + TEST_TOPIC + " was not null");
    assertNotNull(batchesMap.get(topicName), "batch map entry for " + topicName + " was null");

    assertAll(
        "verify contents of created batches map",
        () ->
            assertEquals(
                1,
                batchesMap.get(topicName).getBufferedBatches().size(),
                "wrong number of batches in map entry for " + topicName),
        () ->
            assertEquals(
                settings.numRecords,
                batchesMap.get(topicName).getBufferedBatches().get(0).size(),
                "wrong number of records in single batch of map entry for " + topicName),
        () ->
            assertEquals(
                sinkRecords,
                batchesMap.get(topicName).getBufferedBatches().get(0),
                "sink record list mismatch in single batch of map entry for" + topicName));
  }

  @TestFactory
  @DisplayName(
      "test create sink record batches per topic with multiple topics and different batch sizes")
  Collection<DynamicTest>
      testCreateSinkRecordBatchesPerTopicWithMultipleTopicAndDifferentBatchSizes() {
    List<DynamicTest> tests = new ArrayList<>();
    MongoSinkTask sinkTask = new MongoSinkTask();

    List<TopicSettingsAndResults> settings =
        asList(
            new TopicSettingsAndResults("topicA", 10, 2),
            new TopicSettingsAndResults("topicB", 25, 7),
            new TopicSettingsAndResults("topicC", 50, 11));

    Map<String, String> map = createConfigMap(TOPICS_CONFIG, "topicA,topicB,topicC");
    settings.forEach(
        ts ->
            map.put(
                format(TOPIC_OVERRIDE_CONFIG, ts.topic, MAX_BATCH_SIZE_CONFIG),
                String.valueOf(ts.batchSize)));

    sinkTask.start(map);

    List<SinkRecord> allRecords = new ArrayList<>();
    settings.forEach(
        ts -> {
          List<SinkRecord> sinkRecords = createSinkRecordList(ts);
          allRecords.addAll(sinkRecords);
          ts.setSinkRecords(sinkRecords);
          ts.setExpectedBatching(partition(sinkRecords, ts.batchSize));
        });
    Map<String, RecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(allRecords);
    assertEquals(settings.size(), batchesMap.size(), "wrong number of entries in batch map");

    settings.forEach(
        ts ->
            tests.add(
                dynamicTest(
                    "verify contents of created batches map for " + ts.topic,
                    () -> {
                      RecordBatches batches = batchesMap.get(ts.topic);
                      assertNotNull(batches, "batches was null");
                      assertEquals(
                          ts.expectedBatching.size(),
                          batches.getBufferedBatches().size(),
                          "wrong number of batches for map entry " + ts.topic);
                      assertAll(
                          "asserting created batches for equality" + ts.topic,
                          Stream.iterate(0, b -> b + 1)
                              .limit(batches.getBufferedBatches().size())
                              .map(
                                  b ->
                                      (Executable)
                                          () ->
                                              assertEquals(
                                                  ts.expectedBatching.get(b),
                                                  batches.getBufferedBatches().get(b),
                                                  "records mismatch in batch "
                                                      + b
                                                      + " for map entry "
                                                      + ts.topic))
                              .collect(Collectors.toList()));
                    })));
    return tests;
  }

  @Test
  @DisplayName("test with default config and no sink records")
  void testBuildWriteModelDefaultConfigSinkRecordsAbsent() {
    List<? extends WriteModel> writeModelList =
        new MongoSinkTask().buildWriteModel(createTopicConfig(), emptyList());

    assertNotNull(writeModelList, "WriteModel list was null");
    assertEquals(emptyList(), writeModelList, "WriteModel list mismatch");
  }

  @Test
  @DisplayName("test ReplaceOneDefaultStrategy with custom config and sink records having values")
  void testBuildReplaceOneModelsCustomConfigSinkRecordsWithValuePresent() {
    MongoSinkTask sinkTask = new MongoSinkTask();
    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                "{'%s': '%s', '%s': '%s'}",
                DOCUMENT_ID_STRATEGY_CONFIG,
                BsonOidStrategy.class.getName(),
                WRITEMODEL_STRATEGY_CONFIG,
                ReplaceOneDefaultStrategy.class.getName()));

    TopicSettingsAndResults settings = new TopicSettingsAndResults(TEST_TOPIC, 10, 0);
    Schema valueSchema = SchemaBuilder.struct().field("myValueField", Schema.INT32_SCHEMA);
    settings.setValueSchema(valueSchema);
    settings.setValue(new Struct(valueSchema).put("myValueField", 42));

    List<SinkRecord> sinkRecordList = createSinkRecordList(settings);
    List<? extends WriteModel> writeModelList = sinkTask.buildWriteModel(cfg, sinkRecordList);

    assertNotNull(writeModelList, "WriteModel list was null");
    assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");
    assertAll(
        "checking all generated WriteModel entries",
        writeModelList.stream()
            .map(
                wm ->
                    () -> {
                      assertTrue(wm instanceof ReplaceOneModel);
                      ReplaceOneModel<BsonDocument> model = (ReplaceOneModel<BsonDocument>) wm;
                      assertTrue(
                          model.getReplaceOptions().isUpsert(),
                          "replacement expected to be done in upsert mode");
                      BsonDocument filter =
                          model.getFilter().toBsonDocument(BsonDocument.class, null);
                      assertEquals(
                          1, filter.size(), "filter document should only contain " + "_id");
                      assertTrue(
                          filter.get("_id").isObjectId(),
                          "filter document _id was not of type ObjectId");
                      assertTrue(
                          model.getReplacement().get("_id").isObjectId(),
                          "replacement document _id was not of type ObjectId");
                      assertEquals(
                          42,
                          model.getReplacement().getInt32("myValueField").getValue(),
                          "myValueField's value mismatch");
                    }));
  }

  @Test
  @DisplayName(
      "test ReplaceOneDefaultStrategy with custom config and sink records having keys & values")
  void testBuildReplaceOneModelsCustomConfigSinkRecordsWithKeyAndValuePresent() {
    MongoSinkTask sinkTask = new MongoSinkTask();
    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                "{'%s': '%s', '%s': '%s'}",
                DOCUMENT_ID_STRATEGY_CONFIG,
                FullKeyStrategy.class.getName(),
                WRITEMODEL_STRATEGY_CONFIG,
                ReplaceOneDefaultStrategy.class.getName()));

    TopicSettingsAndResults settings = new TopicSettingsAndResults(TEST_TOPIC, 10, 0);
    Schema keySchema = SchemaBuilder.struct().field("myKeyField", Schema.STRING_SCHEMA);
    settings.setKeySchema(keySchema);
    settings.setKey(new Struct(keySchema).put("myKeyField", "ABCD-1234"));
    Schema valueSchema = SchemaBuilder.struct().field("myValueField", Schema.INT32_SCHEMA);
    settings.setValueSchema(valueSchema);
    settings.setValue(new Struct(valueSchema).put("myValueField", 23));

    List<SinkRecord> sinkRecordList = createSinkRecordList(settings);
    List<? extends WriteModel> writeModelList = sinkTask.buildWriteModel(cfg, sinkRecordList);

    assertNotNull(writeModelList, "WriteModel list was null");
    assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");
    assertAll(
        "checking all generated WriteModel entries",
        writeModelList.stream()
            .map(
                wm ->
                    () -> {
                      assertTrue(wm instanceof ReplaceOneModel);
                      ReplaceOneModel<BsonDocument> model = (ReplaceOneModel<BsonDocument>) wm;
                      assertTrue(
                          model.getReplaceOptions().isUpsert(),
                          "replacement expected to be done in upsert mode");
                      BsonDocument filter =
                          model.getFilter().toBsonDocument(BsonDocument.class, null);
                      assertEquals(BsonDocument.parse("{_id: {myKeyField: 'ABCD-1234'}}"), filter);
                      assertEquals(
                          BsonDocument.parse("{_id: {myKeyField: 'ABCD-1234'}, myValueField: 23}"),
                          model.getReplacement());
                    }));
  }

  @Test
  @DisplayName(
      "test DeleteOneDefaultStrategy with custom config and sink records with keys and null values")
  void testBuildDeleteOneModelsCustomConfigSinkRecordsWithKeyAndNullValuePresent() {
    MongoSinkTask sinkTask = new MongoSinkTask();
    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                "{'%s': '%s', '%s': '%s', '%s': %s}",
                DOCUMENT_ID_STRATEGY_CONFIG,
                FullKeyStrategy.class.getName(),
                WRITEMODEL_STRATEGY_CONFIG,
                ReplaceOneDefaultStrategy.class.getName(),
                DELETE_ON_NULL_VALUES_CONFIG,
                "true"));

    TopicSettingsAndResults settings = new TopicSettingsAndResults(TEST_TOPIC, 10, 0);
    Schema keySchema = SchemaBuilder.struct().field("myKeyField", Schema.STRING_SCHEMA);
    settings.setKeySchema(keySchema);
    settings.setKey(new Struct(keySchema).put("myKeyField", "ABCD-1234"));

    List<SinkRecord> sinkRecordList = createSinkRecordList(settings);
    List<? extends WriteModel> writeModelList = sinkTask.buildWriteModel(cfg, sinkRecordList);

    assertNotNull(writeModelList, "WriteModel list was null");
    assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");
    assertAll(
        "checking all generated WriteModel entries",
        writeModelList.stream()
            .map(
                wm ->
                    () -> {
                      assertTrue(wm instanceof DeleteOneModel);
                      DeleteOneModel<BsonDocument> dom = (DeleteOneModel<BsonDocument>) wm;
                      BsonDocument filter =
                          dom.getFilter().toBsonDocument(BsonDocument.class, null);
                      assertEquals(BsonDocument.parse("{_id: {myKeyField: 'ABCD-1234'}}"), filter);
                    }));
  }

  @Test
  @DisplayName("test build WriteModelCDC for Rdbms Insert")
  void testBuildWriteModelCdcForRdbmsInsert() {
    Schema keySchema = getRdbmsKeySchemaSample();
    Schema valueSchema = getRdbmsValueSchemaSample();
    List<SinkRecord> sinkRecords =
        IntStream.iterate(1234, i -> i + 1)
            .limit(5)
            .mapToObj(
                i ->
                    new SinkRecord(
                        "test-topic",
                        0,
                        keySchema,
                        new Struct(keySchema).put("id", i),
                        valueSchema,
                        new Struct(valueSchema)
                            .put("op", "c")
                            .put("before", null)
                            .put(
                                "after",
                                new Struct(valueSchema.field("after").schema())
                                    .put("id", i)
                                    .put("first_name", "Alice_" + i)
                                    .put("last_name", "van Wonderland")
                                    .put("email", "alice_" + i + "@wonder.land"))
                            .put("source", "currently ignored"),
                        i - 1234))
            .collect(Collectors.toList());

    MongoSinkTask sinkTask = new MongoSinkTask();
    String topic = "dbserver1.catalogA.tableB";
    MongoSinkTopicConfig cfg =
        SinkTestHelper.createSinkConfig(
                format(
                    "{'%s': '%s', '%s': '%s'}",
                    TOPICS_CONFIG,
                    topic,
                    CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                    RdbmsHandler.class.getName()))
            .getMongoSinkTopicConfig(topic);
    List<? extends WriteModel> writeModels = sinkTask.buildWriteModelCDC(cfg, sinkRecords);

    assertNotNull(writeModels, "WriteModel list was null");
    assertFalse(writeModels.isEmpty(), "WriteModel list was empty");
    assertAll(
        "checking all generated WriteModel entries",
        IntStream.iterate(1234, i -> i + 1)
            .limit(5)
            .mapToObj(
                i ->
                    () -> {
                      int index = i - 1234;
                      WriteModel wm = writeModels.get(index);
                      assertNotNull(wm, "WriteModel at index " + index + " must not be null");
                      assertTrue(wm instanceof ReplaceOneModel);

                      ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>) wm;
                      assertTrue(
                          rom.getReplaceOptions().isUpsert(),
                          "replacement expected to be done in upsert mode");

                      BsonDocument filter =
                          rom.getFilter().toBsonDocument(BsonDocument.class, null);
                      assertEquals(BsonDocument.parse(format("{_id: {id: %s}}", i)), filter);
                      assertEquals(
                          BsonDocument.parse(
                              format(
                                  "{_id: {id: %s}, first_name: 'Alice_%s', last_name: 'van Wonderland',"
                                      + "email: 'alice_%s@wonder.land'}",
                                  i, i, i)),
                          rom.getReplacement());
                    }));
  }

  @Test
  @DisplayName("test build WriteModelCDC for Rdbms Update")
  void testBuildWriteModelCdcForRdbmsUpdate() {
    String topic = "dbserver1.catalogA.tableB";
    Schema keySchema = getRdbmsKeySchemaSample();
    Schema valueSchema = getRdbmsValueSchemaSample();
    List<SinkRecord> sinkRecords =
        IntStream.iterate(1234, i -> i + 1)
            .limit(5)
            .mapToObj(
                i ->
                    new SinkRecord(
                        topic,
                        0,
                        keySchema,
                        new Struct(keySchema).put("id", i),
                        valueSchema,
                        new Struct(valueSchema)
                            .put("op", "u")
                            .put(
                                "before",
                                new Struct(valueSchema.field("before").schema())
                                    .put("id", i)
                                    .put("first_name", "Alice_" + i)
                                    .put("last_name", "van Wonderland")
                                    .put("email", "alice_" + i + "@wonder.land"))
                            .put(
                                "after",
                                new Struct(valueSchema.field("after").schema())
                                    .put("id", i)
                                    .put("first_name", "Alice" + i)
                                    .put("last_name", "in Wonderland")
                                    .put("email", "alice" + i + "@wonder.land"))
                            .put("source", "Ignored"),
                        i - 1234))
            .collect(Collectors.toList());

    MongoSinkTask sinkTask = new MongoSinkTask();
    MongoSinkTopicConfig cfg =
        SinkTestHelper.createSinkConfig(
                format(
                    "{'%s': '%s', '%s': '%s'}",
                    TOPICS_CONFIG,
                    topic,
                    CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                    RdbmsHandler.class.getName()))
            .getMongoSinkTopicConfig(topic);
    List<? extends WriteModel> writeModels = sinkTask.buildWriteModelCDC(cfg, sinkRecords);

    assertNotNull(writeModels, "WriteModel list was null");
    assertFalse(writeModels.isEmpty(), "WriteModel list was empty");
    assertAll(
        "checking all generated WriteModel entries",
        IntStream.iterate(1234, i -> i + 1)
            .limit(5)
            .mapToObj(
                i ->
                    () -> {
                      int index = i - 1234;
                      WriteModel wm = writeModels.get(index);
                      assertNotNull(wm, "WriteModel at index " + index + " must not be null");
                      assertTrue(wm instanceof ReplaceOneModel);

                      ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>) wm;
                      assertTrue(
                          rom.getReplaceOptions().isUpsert(),
                          "replacement expected to be done in upsert mode");

                      BsonDocument filter =
                          rom.getFilter().toBsonDocument(BsonDocument.class, null);
                      assertEquals(BsonDocument.parse(format("{_id: {id: %s}}", i)), filter);
                      assertEquals(
                          BsonDocument.parse(
                              format(
                                  "{_id: {id: %s}, first_name: 'Alice%s', last_name: 'in Wonderland',"
                                      + "email: 'alice%s@wonder.land'}",
                                  i, i, i)),
                          rom.getReplacement());
                    }));
  }

  @Test
  @DisplayName("test build WriteModelCDC for Rdbms Delete")
  void testBuildWriteModelCdcForRdbmsDelete() {
    String topic = "dbserver1.catalogA.tableB";
    Schema keySchema = getRdbmsKeySchemaSample();
    Schema valueSchema = getRdbmsValueSchemaSample();
    List<SinkRecord> sinkRecords =
        IntStream.iterate(1234, i -> i + 1)
            .limit(5)
            .mapToObj(
                i ->
                    new SinkRecord(
                        topic,
                        0,
                        keySchema,
                        new Struct(keySchema).put("id", i),
                        valueSchema,
                        new Struct(valueSchema)
                            .put("op", "d")
                            .put(
                                "before",
                                new Struct(valueSchema.field("before").schema())
                                    .put("id", i)
                                    .put("first_name", "Alice" + i)
                                    .put("last_name", "in Wonderland")
                                    .put("email", "alice" + i + "@wonder.land"))
                            .put("after", null)
                            .put("source", "ignored"),
                        i - 1234))
            .collect(Collectors.toList());

    MongoSinkTask sinkTask = new MongoSinkTask();
    MongoSinkTopicConfig cfg =
        SinkTestHelper.createSinkConfig(
                format(
                    "{'%s': '%s', '%s': '%s'}",
                    TOPICS_CONFIG,
                    topic,
                    CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                    RdbmsHandler.class.getName()))
            .getMongoSinkTopicConfig(topic);
    List<? extends WriteModel> writeModels = sinkTask.buildWriteModelCDC(cfg, sinkRecords);
    assertNotNull(writeModels, "WriteModel list was null");
    assertFalse(writeModels.isEmpty(), "WriteModel list was empty");
    assertAll(
        "checking all generated WriteModel entries",
        IntStream.iterate(1234, i -> i + 1)
            .limit(5)
            .mapToObj(
                i ->
                    () -> {
                      int index = i - 1234;
                      WriteModel wm = writeModels.get(index);
                      assertNotNull(wm, "WriteModel at index " + index + " must not be null");
                      assertTrue(wm instanceof DeleteOneModel);

                      DeleteOneModel<BsonDocument> rom = (DeleteOneModel<BsonDocument>) wm;
                      BsonDocument filter =
                          rom.getFilter().toBsonDocument(BsonDocument.class, null);
                      assertEquals(BsonDocument.parse(format("{_id: {id: %s}}", i)), filter);
                    }));
  }

  private static Schema getRdbmsKeySchemaSample() {
    return SchemaBuilder.struct()
        .name("dbserver1.catalogA.tableB.Key")
        .field("id", Schema.INT32_SCHEMA)
        .build();
  }

  private static Schema getRdbmsValueSchemaSample() {
    return SchemaBuilder.struct()
        .name("dbserver1.catalogA.tableB.Envelope")
        .field("op", Schema.STRING_SCHEMA)
        .field(
            "before",
            SchemaBuilder.struct()
                .name("dbserver1.catalogA.tableB.Value")
                .optional()
                .field("id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA))
        .field(
            "after",
            SchemaBuilder.struct()
                .name("dbserver1.catalogA.tableB.Value")
                .optional()
                .field("id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA))
        .field("source", Schema.STRING_SCHEMA);
  }

  private static List<SinkRecord> createSinkRecordList(final TopicSettingsAndResults settings) {
    return Stream.iterate(0, r -> r + 1)
        .limit(settings.numRecords)
        .map(
            r ->
                new SinkRecord(
                    settings.topic,
                    0,
                    settings.keySchema,
                    settings.key,
                    settings.valueSchema,
                    settings.value,
                    r))
        .collect(Collectors.toList());
  }

  private static <T> List<List<T>> partition(final List<T> list, final int size) {
    final AtomicInteger counter = new AtomicInteger(0);
    return new ArrayList<>(
        list.stream()
            .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size))
            .values());
  }

  private static class TopicSettingsAndResults {

    private final String topic;
    private final int numRecords;
    private final int batchSize;

    private Schema keySchema;
    private Object key;
    private Schema valueSchema;
    private Object value;

    private List<SinkRecord> sinkRecords;
    private List<List<SinkRecord>> expectedBatching;

    TopicSettingsAndResults(final String topic, final int numRecords, final int batchSize) {
      this.topic = topic;
      this.numRecords = numRecords;
      this.batchSize = batchSize;
    }

    void setSinkRecords(final List<SinkRecord> sinkRecords) {
      this.sinkRecords = sinkRecords;
    }

    void setExpectedBatching(final List<List<SinkRecord>> expectedBatching) {
      this.expectedBatching = expectedBatching;
    }

    void setKeySchema(final Schema keySchema) {
      this.keySchema = keySchema;
    }

    void setKey(final Object key) {
      this.key = key;
    }

    void setValueSchema(final Schema valueSchema) {
      this.valueSchema = valueSchema;
    }

    void setValue(final Object value) {
      this.value = value;
    }
  }
}
