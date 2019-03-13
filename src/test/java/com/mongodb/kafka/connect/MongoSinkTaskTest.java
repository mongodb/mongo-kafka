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

package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.COLLECTIONS_CONFIG;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.DATABASE_NAME_CONFIG;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.DELETE_ON_NULL_VALUES_CONFIG;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.DOCUMENT_ID_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.FIELD_LIST_SPLIT_CHAR;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.WRITEMODEL_STRATEGY_CONFIG;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.bson.BsonInt32;
import org.bson.BsonString;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.cdc.debezium.rdbms.RdbmsHandler;
import com.mongodb.kafka.connect.processor.id.strategy.BsonOidStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.FullKeyStrategy;
import com.mongodb.kafka.connect.writemodel.strategy.ReplaceOneDefaultStrategy;


@SuppressWarnings("unchecked")
@RunWith(JUnitPlatform.class)
class MongoSinkTaskTest {

    private static class TopicSettingsAndResults {

        private final String topic;
        private final String collection;
        private final int numRecords;
        private final int batchSize;

        private Schema keySchema;
        private Object key;
        private Schema valueSchema;
        private Object value;

        private List<SinkRecord> sinkRecords;
        private List<List<SinkRecord>> expectedBatching;

        TopicSettingsAndResults(final String topic, final String collection, final int numRecords, final int batchSize) {
            this.topic = topic;
            this.collection = collection;
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

    private static final String DATABASE_NAME = "MongoKafka";
    private Map<String, String> getDefaultProps() {
        Map<String, String> props = new HashMap<>();
        props.put(DATABASE_NAME_CONFIG, DATABASE_NAME);
        return props;
    }

    @Test
    @DisplayName("test create sink record batches per topic with default topic and no batching")
    void testCreateSinkRecordBatchesPerTopicWithDefaultTopicAndNoBatching() {

        MongoSinkTask sinkTask = new MongoSinkTask();

        TopicSettingsAndResults settings = new TopicSettingsAndResults("kafkatopic", "kafkatopic", 50, 0);
        String namespace = DATABASE_NAME + "." + settings.collection;

        Map<String, String> props = getDefaultProps();
        props.put(COLLECTION_CONFIG, settings.collection);
        sinkTask.start(props);

        List<SinkRecord> sinkRecords = createSinkRecordList(settings);
        Map<String, MongoSinkRecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

        assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
        assertNotNull(batchesMap.get(namespace), "batch map entry for " + namespace + " was null");

        assertAll("verify contents of created batches map",
                () -> assertEquals(1, batchesMap.get(namespace).getBufferedBatches().size(),
                        "wrong number of batches in map entry for " + namespace),
                () -> assertEquals(settings.numRecords, batchesMap.get(namespace).getBufferedBatches().get(0).size(),
                        "wrong number of records in single batch of map entry for " + namespace),
                () -> assertEquals(sinkRecords, batchesMap.get(namespace).getBufferedBatches().get(0),
                        "sink record list mismatch in single batch of map entry for" + namespace)
        );

    }

    @Test
    @DisplayName("test create sink record batches per topic with NO default topic and no batching")
    void testCreateSinkRecordBatchesPerTopicWithNoDefaultTopicAndNoBatching() {

        MongoSinkTask sinkTask = new MongoSinkTask();

        TopicSettingsAndResults settings = new TopicSettingsAndResults("kafkaesque", "kafkaesque", 50, 0);
        String namespace = DATABASE_NAME + "." + settings.collection;

        sinkTask.start(getDefaultProps());

        List<SinkRecord> sinkRecords = createSinkRecordList(settings);
        Map<String, MongoSinkRecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

        assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
        assertNotNull(batchesMap.get(namespace), "batch map entry for " + namespace + " was null");

        assertAll("verify contents of created batches map",
                () -> assertEquals(1, batchesMap.get(namespace).getBufferedBatches().size(),
                        "wrong number of batches in map entry for " + namespace),
                () -> assertEquals(settings.numRecords, batchesMap.get(namespace).getBufferedBatches().get(0).size(),
                        "wrong number of records in single batch of map entry for " + namespace),
                () -> assertEquals(sinkRecords, batchesMap.get(namespace).getBufferedBatches().get(0),
                        "sink record list mismatch in single batch of map entry for" + namespace)
        );

    }

    @Test
    @DisplayName("test create sink record batches per topic with single topic and no batching")
    void testCreateSinkRecordBatchesPerTopicWithSingleTopicAndNoBatching() {

        MongoSinkTask sinkTask = new MongoSinkTask();

        TopicSettingsAndResults settings = new TopicSettingsAndResults("foo-topic", "foo-collection", 100, 0);
        String namespace = DATABASE_NAME + "." + settings.collection;

        Map<String, String> props = getDefaultProps();
        props.put(COLLECTIONS_CONFIG, settings.collection);
        props.put(COLLECTION_CONFIG + "." + settings.topic, settings.collection);
        props.put(MAX_BATCH_SIZE_CONFIG + "." + settings.collection, String.valueOf(settings.batchSize));
        sinkTask.start(props);

        List<SinkRecord> sinkRecords = createSinkRecordList(settings);
        Map<String, MongoSinkRecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(sinkRecords);

        assertEquals(1, batchesMap.size(), "wrong number of entries in batch map");
        assertNotNull(batchesMap.get(namespace), "batch map entry for " + namespace + " was null");

        assertAll("verify contents of created batches map",
                () -> assertEquals(1, batchesMap.get(namespace).getBufferedBatches().size(),
                        "wrong number of batches in map entry for " + namespace),
                () -> assertEquals(settings.numRecords, batchesMap.get(namespace).getBufferedBatches().get(0).size(),
                        "wrong number of records in single batch of map entry for " + namespace),
                () -> assertEquals(sinkRecords, batchesMap.get(namespace).getBufferedBatches().get(0),
                        "sink record list mismatch in single batch of map entry for" + namespace)
        );

    }

    @TestFactory
    @DisplayName("test create sink record batches per topic with multiple topics and different batch sizes")
    Collection<DynamicTest> testCreateSinkRecordBatchesPerTopicWithMultipleTopicAndDifferentBatchSizes() {

        List<DynamicTest> tests = new ArrayList<>();

        MongoSinkTask sinkTask = new MongoSinkTask();

        List<TopicSettingsAndResults> settings = asList(
                new TopicSettingsAndResults("foo-topic", "foo-collection", 10, 2),
                new TopicSettingsAndResults("blah-topic", "blah-collection", 25, 7),
                new TopicSettingsAndResults("xyz-topic", "xyz-collection", 50, 11)
        );

        Map<String, String> props = getDefaultProps();
        props.put(COLLECTIONS_CONFIG,
                settings.stream()
                        .map(ts -> ts.collection)
                        .collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)));

        settings.forEach(ts -> {
            props.put(COLLECTION_CONFIG + "." + ts.topic, ts.collection);
            props.put(MAX_BATCH_SIZE_CONFIG + "." + ts.collection, String.valueOf(ts.batchSize));
        });

        List<SinkRecord> allRecords = new ArrayList<>();

        settings.forEach(ts -> {
                    List<SinkRecord> sinkRecords = createSinkRecordList(ts);
                    allRecords.addAll(sinkRecords);
                    ts.setSinkRecords(sinkRecords);
                    ts.setExpectedBatching(partition(sinkRecords, ts.batchSize));
                }
        );

        sinkTask.start(props);

        Map<String, MongoSinkRecordBatches> batchesMap = sinkTask.createSinkRecordBatchesPerTopic(allRecords);
        assertEquals(settings.size(), batchesMap.size(), "wrong number of entries in batch map");

        settings.forEach(ts -> {
            String namespace = DATABASE_NAME + "." + ts.collection;
            tests.add(dynamicTest("verify contents of created batches map for " + namespace, () -> {
                MongoSinkRecordBatches batches = batchesMap.get(namespace);
                assertNotNull(batches, "batches was null");
                assertEquals(ts.expectedBatching.size(), batches.getBufferedBatches().size(),
                        "wrong number of batches for map entry " + namespace);
                assertAll("asserting created batches for equality" + namespace,
                        Stream.iterate(0, b -> b + 1)
                                .limit(batches.getBufferedBatches().size())
                                .map(b -> (Executable) () ->
                                        assertEquals(ts.expectedBatching.get(b), batches.getBufferedBatches().get(b),
                                                "records mismatch in batch " + b + " for map entry " + namespace))
                                .collect(Collectors.toList())
                );
            }));
        });

        return tests;
    }

    @Test
    @DisplayName("test with default config and no sink records")
    void testBuildWriteModelDefaultConfigSinkRecordsAbsent() {
        MongoSinkTask sinkTask = new MongoSinkTask();
        sinkTask.start(getDefaultProps());

        List<? extends WriteModel> writeModelList = sinkTask.buildWriteModel(new ArrayList<>(), "kafkatopic");

        assertNotNull(writeModelList, "WriteModel list was null");
        assertEquals(Collections.emptyList(), writeModelList, "WriteModel list mismatch");
    }

    @Test
    @DisplayName("test ReplaceOneDefaultStragey with custom config and sink records having values")
    void testBuildReplaceOneModelsCustomConfigSinkRecordsWithValuePresent() {
        MongoSinkTask sinkTask = new MongoSinkTask();
        Map<String, String> props = getDefaultProps();
        props.put(DOCUMENT_ID_STRATEGY_CONFIG, BsonOidStrategy.class.getName());
        props.put(WRITEMODEL_STRATEGY_CONFIG, ReplaceOneDefaultStrategy.class.getName());
        sinkTask.start(props);

        TopicSettingsAndResults settings = new TopicSettingsAndResults("kafkatopic", "kafkatopic", 10, 0);

        Schema vs = SchemaBuilder.struct()
                .field("myValueField", Schema.INT32_SCHEMA);
        settings.setValueSchema(vs);
        settings.setValue(new Struct(vs)
                .put("myValueField", 42));

        List<SinkRecord> sinkRecordList = createSinkRecordList(settings);

        List<? extends WriteModel> writeModelList = sinkTask.buildWriteModel(sinkRecordList, "kafkatopic");

        assertNotNull(writeModelList, "WriteModel list was null");
        assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");
        assertAll("checking all generated WriteModel entries",
                writeModelList.stream().map(wm ->
                        () -> assertAll("assertions for single WriteModel",
                                () -> assertTrue(wm instanceof ReplaceOneModel),
                                () -> {
                                    ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>) wm;
                                    assertTrue(rom.getReplaceOptions().isUpsert(), "replacement expected to be done in upsert mode");
                                    BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class, null);
                                    assertEquals(1, filter.size(), "filter document should only contain " + "_id");
                                    assertTrue(filter.get("_id").isObjectId(), "filter document _id was not of type ObjectId");
                                    assertTrue(rom.getReplacement().get("_id").isObjectId(),
                                            "replacement document _id was not of type ObjectId");
                                    assertEquals(42, rom.getReplacement().getInt32("myValueField").getValue(),
                                            "myValueField's value mismatch");
                                }
                        )
                )
        );

    }

    @Test
    @DisplayName("test ReplaceOneDefaultStrategy with custom config and sink records having keys & values")
    void testBuildReplaceOneModelsCustomConfigSinkRecordsWithKeyAndValuePresent() {

        MongoSinkTask sinkTask = new MongoSinkTask();
        Map<String, String> props = getDefaultProps();
        props.put(DOCUMENT_ID_STRATEGY_CONFIG, FullKeyStrategy.class.getName());
        props.put(WRITEMODEL_STRATEGY_CONFIG, ReplaceOneDefaultStrategy.class.getName());
        props.put("topics", "blah");
        props.put(COLLECTIONS_CONFIG, "blah-collection");
        props.put(COLLECTION_CONFIG + "." + "blah", "blah-collection");
        sinkTask.start(props);

        TopicSettingsAndResults settings = new TopicSettingsAndResults("blah", "blah-collection", 10, 0);

        Schema ks = SchemaBuilder.struct()
                .field("myKeyField", Schema.STRING_SCHEMA);
        settings.setKeySchema(ks);
        settings.setKey(new Struct(ks)
                .put("myKeyField", "ABCD-1234"));
        Schema vs = SchemaBuilder.struct()
                .field("myValueField", Schema.INT32_SCHEMA);
        settings.setValueSchema(vs);
        settings.setValue(new Struct(vs)
                .put("myValueField", 23));

        List<SinkRecord> sinkRecordList = createSinkRecordList(settings);

        List<? extends WriteModel> writeModelList =
                sinkTask.buildWriteModel(sinkRecordList, "blah-collection");

        assertNotNull(writeModelList, "WriteModel list was null");

        assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                writeModelList.stream().map(wm ->
                        () -> assertAll("assertions for single WriteModel",
                                () -> assertTrue(wm instanceof ReplaceOneModel),
                                () -> {
                                    ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>) wm;
                                    assertTrue(rom.getReplaceOptions().isUpsert(), "replacement expected to be done in upsert mode");
                                    BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class, null);
                                    assertEquals(new BsonDocument("_id",
                                                    new BsonDocument("myKeyField", new BsonString("ABCD-1234"))),
                                            filter);
                                    assertEquals(new BsonDocument("_id",
                                                    new BsonDocument("myKeyField", new BsonString("ABCD-1234")))
                                                    .append("myValueField", new BsonInt32(23)),
                                            rom.getReplacement());
                                }
                        )
                )
        );

    }

    @Test
    @DisplayName("test DeleteOneDefaultStrategy with custom config and sink records with keys and null values")
    void testBuildDeleteOneModelsCustomConfigSinkRecordsWithKeyAndNullValuePresent() {

        MongoSinkTask sinkTask = new MongoSinkTask();
        Map<String, String> props = getDefaultProps();
        props.put(DOCUMENT_ID_STRATEGY_CONFIG, FullKeyStrategy.class.getName());
        props.put(WRITEMODEL_STRATEGY_CONFIG, ReplaceOneDefaultStrategy.class.getName());
        props.put(DELETE_ON_NULL_VALUES_CONFIG, "true");
        props.put("topics", "foo");
        props.put(COLLECTIONS_CONFIG, "foo-collection");
        props.put(COLLECTION_CONFIG + "." + "foo", "foo-collection");
        sinkTask.start(props);

        TopicSettingsAndResults settings = new TopicSettingsAndResults("foo", "foo-collection", 10, 0);

        Schema ks = SchemaBuilder.struct()
                .field("myKeyField", Schema.STRING_SCHEMA);
        settings.setKeySchema(ks);
        settings.setKey(new Struct(ks)
                .put("myKeyField", "ABCD-1234"));

        List<SinkRecord> sinkRecordList = createSinkRecordList(settings);

        List<? extends WriteModel> writeModelList =
                sinkTask.buildWriteModel(sinkRecordList, "blah-collection");

        assertNotNull(writeModelList, "WriteModel list was null");

        assertFalse(writeModelList.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                writeModelList.stream().map(wm ->
                        () -> assertAll("assertions for single WriteModel",
                                () -> assertTrue(wm instanceof DeleteOneModel),
                                () -> {
                                    DeleteOneModel<BsonDocument> dom = (DeleteOneModel<BsonDocument>) wm;
                                    BsonDocument filter = dom.getFilter().toBsonDocument(BsonDocument.class, null);
                                    assertEquals(new BsonDocument("_id",
                                                    new BsonDocument("myKeyField", new BsonString("ABCD-1234"))),
                                            filter);
                                }
                        )
                )
        );

    }

    @Test
    @DisplayName("test build WriteModelCDC for Rdbms Insert")
    void testBuildWriteModelCdcForRdbmsInsert() {
        Schema keySchema = getRdbmsKeySchemaSample();
        Schema valueSchema = getRdbmsValueSchemaSample();
        List<SinkRecord> sinkRecords = IntStream.iterate(1234, i -> i + 1).limit(5)
                .mapToObj(i -> new SinkRecord("test-topic", 0,
                        keySchema, new Struct(keySchema)
                        .put("id", i),
                        valueSchema, new Struct(valueSchema)
                        .put("op", "c")
                        .put("before", null)
                        .put("after",
                                new Struct(valueSchema.field("after").schema())
                                        .put("id", i)
                                        .put("first_name", "Alice_" + i)
                                        .put("last_name", "van Wonderland")
                                        .put("email", "alice_" + i + "@wonder.land")),
                        //.put("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
                        i - 1234
                ))
                .collect(Collectors.toList());

        MongoSinkTask sinkTask = new MongoSinkTask();
        Map<String, String> props = getDefaultProps();
        props.put("topics", "dbserver1.catalogA.tableB");
        props.put(COLLECTIONS_CONFIG, "dbserver1.catalogA.tableB");
        props.put(COLLECTION_CONFIG + "." + "dbserver1.catalogA.tableB", "dbserver1.catalogA.tableB");
        props.put(CHANGE_DATA_CAPTURE_HANDLER_CONFIG + "." + "dbserver1.catalogA.tableB", RdbmsHandler.class.getName());
        sinkTask.start(props);

        List<? extends WriteModel> writeModels = sinkTask.buildWriteModelCDC(sinkRecords, "dbserver1.catalogA.tableB");

        assertNotNull(writeModels, "WriteModel list was null");
        assertFalse(writeModels.isEmpty(), "WriteModel list was empty");
        assertAll("checking all generated WriteModel entries",
                IntStream.iterate(1234, i -> i + 1).limit(5).mapToObj(
                        i -> () -> {
                            int index = i - 1234;
                            WriteModel wm = writeModels.get(index);
                            assertNotNull(wm, "WriteModel at index " + index + " must not be null");
                            assertTrue(wm instanceof ReplaceOneModel);
                            ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>) wm;
                            assertTrue(rom.getReplaceOptions().isUpsert(), "replacement expected to be done in upsert mode");
                            BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class, null);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id", new BsonInt32(i))),
                                    filter);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id", new BsonInt32(i)))
                                            .append("first_name", new BsonString("Alice_" + i))
                                            .append("last_name", new BsonString("van Wonderland"))
                                            .append("email", new BsonString("alice_" + i + "@wonder.land")),
                                    rom.getReplacement());
                        }
                )
        );

    }

    @Test
    @DisplayName("test build WriteModelCDC for Rdbms Update")
    void testBuildWriteModelCdcForRdbmsUpdate() {

        Schema keySchema = getRdbmsKeySchemaSample();
        Schema valueSchema = getRdbmsValueSchemaSample();
        List<SinkRecord> sinkRecords = IntStream.iterate(1234, i -> i + 1).limit(5)
                .mapToObj(i -> new SinkRecord("test-topic", 0,
                        keySchema, new Struct(keySchema)
                        .put("id", i),
                        valueSchema, new Struct(valueSchema)
                        .put("op", "u")
                        .put("before", new Struct(valueSchema.field("before").schema())
                                .put("id", i)
                                .put("first_name", "Alice_" + i)
                                .put("last_name", "van Wonderland")
                                .put("email", "alice_" + i + "@wonder.land"))
                        .put("after", new Struct(valueSchema.field("after").schema())
                                .put("id", i)
                                .put("first_name", "Alice" + i)
                                .put("last_name", "in Wonderland")
                                .put("email", "alice" + i + "@wonder.land")),
                        //.put("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
                        i - 1234
                ))
                .collect(Collectors.toList());

        MongoSinkTask sinkTask = new MongoSinkTask();
        Map<String, String> props = getDefaultProps();
        props.put("topics", "dbserver1.catalogA.tableB");
        props.put(COLLECTIONS_CONFIG, "dbserver1.catalogA.tableB");
        props.put(COLLECTION_CONFIG + "." + "dbserver1.catalogA.tableB", "dbserver1.catalogA.tableB");
        props.put(CHANGE_DATA_CAPTURE_HANDLER_CONFIG + "." + "dbserver1.catalogA.tableB",
                RdbmsHandler.class.getName());
        sinkTask.start(props);

        List<? extends WriteModel> writeModels =
                sinkTask.buildWriteModelCDC(sinkRecords, "dbserver1.catalogA.tableB");

        assertNotNull(writeModels, "WriteModel list was null");

        assertFalse(writeModels.isEmpty(), "WriteModel list was empty");

        assertAll("checking all generated WriteModel entries",
                IntStream.iterate(1234, i -> i + 1).limit(5).mapToObj(
                        i -> () -> {
                            int index = i - 1234;
                            WriteModel wm = writeModels.get(index);
                            assertNotNull(wm, "WriteModel at index " + index + " must not be null");
                            assertTrue(wm instanceof ReplaceOneModel);
                            ReplaceOneModel<BsonDocument> rom = (ReplaceOneModel<BsonDocument>) wm;
                            assertTrue(rom.getReplaceOptions().isUpsert(), "replacement expected to be done in upsert mode");
                            BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class, null);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id", new BsonInt32(i))),
                                    filter);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id", new BsonInt32(i)))
                                            .append("first_name", new BsonString("Alice" + i))
                                            .append("last_name", new BsonString("in Wonderland"))
                                            .append("email", new BsonString("alice" + i + "@wonder.land")),
                                    rom.getReplacement());
                        }
                )
        );

    }

    @Test
    @DisplayName("test build WriteModelCDC for Rdbms Delete")
    void testBuildWriteModelCdcForRdbmsDelete() {

        Schema keySchema = getRdbmsKeySchemaSample();
        Schema valueSchema = getRdbmsValueSchemaSample();
        List<SinkRecord> sinkRecords = IntStream.iterate(1234, i -> i + 1).limit(5)
                .mapToObj(i -> new SinkRecord("test-topic", 0,
                        keySchema, new Struct(keySchema)
                        .put("id", i),
                        valueSchema, new Struct(valueSchema)
                        .put("op", "d")
                        .put("before", new Struct(valueSchema.field("before").schema())
                                .put("id", i)
                                .put("first_name", "Alice" + i)
                                .put("last_name", "in Wonderland")
                                .put("email", "alice" + i + "@wonder.land"))
                        .put("after", null),
                        //.put("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
                        i - 1234
                ))
                .collect(Collectors.toList());

        MongoSinkTask sinkTask = new MongoSinkTask();
        Map<String, String> props = getDefaultProps();
        props.put("topics", "dbserver1.catalogA.tableB");
        props.put(COLLECTIONS_CONFIG, "dbserver1.catalogA.tableB");
        props.put(COLLECTION_CONFIG + "." + "dbserver1.catalogA.tableB", "dbserver1.catalogA.tableB");
        props.put(CHANGE_DATA_CAPTURE_HANDLER_CONFIG + "." + "dbserver1.catalogA.tableB",
                RdbmsHandler.class.getName());
        sinkTask.start(props);

        List<? extends WriteModel> writeModels = sinkTask.buildWriteModelCDC(sinkRecords, "dbserver1.catalogA.tableB");
        assertNotNull(writeModels, "WriteModel list was null");
        assertFalse(writeModels.isEmpty(), "WriteModel list was empty");
        assertAll("checking all generated WriteModel entries",
                IntStream.iterate(1234, i -> i + 1).limit(5).mapToObj(
                        i -> () -> {
                            int index = i - 1234;
                            WriteModel wm = writeModels.get(index);
                            assertNotNull(wm, "WriteModel at index " + index + " must not be null");
                            assertTrue(wm instanceof DeleteOneModel);
                            DeleteOneModel<BsonDocument> rom = (DeleteOneModel<BsonDocument>) wm;
                            BsonDocument filter = rom.getFilter().toBsonDocument(BsonDocument.class, null);
                            assertEquals(new BsonDocument("_id",
                                            new BsonDocument("id", new BsonInt32(i))),
                                    filter);
                        }
                )
        );

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
                .field("before",
                        SchemaBuilder.struct().name("dbserver1.catalogA.tableB.Value").optional()
                                .field("id", Schema.INT32_SCHEMA)
                                .field("first_name", Schema.STRING_SCHEMA)
                                .field("last_name", Schema.STRING_SCHEMA)
                                .field("email", Schema.STRING_SCHEMA))
                .field("after",
                        SchemaBuilder.struct().name("dbserver1.catalogA.tableB.Value").optional()
                                .field("id", Schema.INT32_SCHEMA)
                                .field("first_name", Schema.STRING_SCHEMA)
                                .field("last_name", Schema.STRING_SCHEMA)
                                .field("email", Schema.STRING_SCHEMA));
                //.field("source",...) //NOTE: SKIPPED SINCE NOT USED AT ALL SO FAR
    }

    private static List<SinkRecord> createSinkRecordList(final TopicSettingsAndResults settings) {
        return Stream.iterate(0, r -> r + 1)
                .limit(settings.numRecords)
                .map(r -> new SinkRecord(settings.topic, 0, settings.keySchema, settings.key, settings.valueSchema, settings.value, r))
                .collect(Collectors.toList());
    }

    private static <T> List<List<T>> partition(final List<T> list, final int size) {
        final AtomicInteger counter = new AtomicInteger(0);
        return new ArrayList<>(list.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / size))
                .values());
    }

}
