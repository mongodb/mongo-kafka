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

package com.mongodb.kafka.connect.source;

import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createChangeStreamOperation;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createDropCollection;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createDropDatabase;
import static com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.createInserts;
import static com.mongodb.kafka.connect.source.schema.SchemaUtils.assertStructsEquals;
import static com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils.getMBeanAttributes;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ChangeStreamPreAndPostImagesOptions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;
import com.mongodb.client.model.changestream.OperationType;

import com.mongodb.kafka.connect.log.LogCapture;
import com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.ChangeStreamOperation;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.source.MongoSourceConfig.ErrorTolerance;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;
import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson;

@ExtendWith(MockitoExtension.class)
public class MongoSourceTaskIntegrationTest extends MongoKafkaTestCase {
  private static final Map<String, Object> INVALID_OFFSET =
      singletonMap(
          "_id",
          "{\"_data\": \"825F58DDF4000000032B022C0100296E5A1004BBCFDF90907247ABA61D94DF01D76200461E5F6964002B020004\"}");

  @Mock private SourceTaskContext context;
  @Mock private OffsetStorageReader offsetStorageReader;

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
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoDatabase db1 = getDatabaseWithPostfix();
      MongoDatabase db2 = getDatabaseWithPostfix();
      MongoDatabase db3 = getDatabaseWithPostfix();
      MongoCollection<Document> coll1 = db1.getCollection("coll");
      MongoCollection<Document> coll2 = db2.getCollection("coll");
      MongoCollection<Document> coll3 = db3.getCollection("coll");
      MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

      task.start(emptyMap());

      insertMany(rangeClosed(1, 75), coll1, coll2);

      List<SourceRecord> firstPoll = getNextResults(task);

      assertAll(
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll1),
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll2));

      db1.drop();
      insertMany(rangeClosed(101, 150), coll2, coll4);
      insertMany(rangeClosed(1, 48), coll3);

      List<SourceRecord> secondPoll = getNextResults(task);
      assertAll(
          () -> assertEquals(150, secondPoll.size()),
          () -> assertSourceRecordValues(singletonList(createDropCollection()), secondPoll, coll1),
          () -> assertSourceRecordValues(singletonList(createDropDatabase()), secondPoll, db1),
          () -> assertSourceRecordValues(createInserts(101, 150), secondPoll, coll2),
          () -> assertSourceRecordValues(createInserts(1, 48), secondPoll, coll3),
          () -> assertSourceRecordValues(createInserts(101, 150), secondPoll, coll4));
    }
  }

  @Test
  @DisplayName("Ensure source loads data from MongoClient with copy existing data")
  void testSourceLoadsDataFromMongoClientWithCopyExisting() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoDatabase db1 = getDatabaseWithPostfix();
      MongoDatabase db2 = getDatabaseWithPostfix();
      MongoDatabase db3 = getDatabaseWithPostfix();
      MongoCollection<Document> coll1 = db1.getCollection("coll");
      MongoCollection<Document> coll2 = db2.getCollection("coll");
      MongoCollection<Document> coll3 = db3.getCollection("coll");
      MongoCollection<Document> coll4 = db1.getCollection("db1Coll2");

      insertMany(rangeClosed(1, 75), coll1, coll2);

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              String namespaceRegex =
                  String.format("(%s\\.coll|%s\\.coll)", db1.getName(), db2.getName());
              put(MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_CONFIG, namespaceRegex);
            }
          };
      task.start(cfg);
      List<SourceRecord> firstPoll = getNextResults(task);

      assertAll(
          () -> assertEquals(150, firstPoll.size()),
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll1),
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll2),
          // make sure all elements, except the last, contains the "copy" key
          () ->
              assertTrue(
                  firstPoll.stream()
                      .map(SourceRecord::sourceOffset)
                      .limit(150 - 1) // exclude the last record
                      .allMatch(i -> i.containsKey("copy"))),
          // make sure that the last copied element does not have the "copy" key
          () ->
              assertTrue(
                  firstPoll.stream()
                      .map(SourceRecord::sourceOffset)
                      .skip(150 - 1) // exclude the last record
                      .findFirst()
                      .filter(i -> !i.containsKey("copy"))
                      .isPresent()));

      assertNull(task.poll());

      db1.drop();
      insertMany(rangeClosed(101, 150), coll2, coll4);
      insertMany(rangeClosed(1, 48), coll3);

      List<SourceRecord> secondPoll = getNextResults(task);
      assertAll(
          () -> assertEquals(150, secondPoll.size()),
          () -> assertSourceRecordValues(singletonList(createDropCollection()), secondPoll, coll1),
          () -> assertSourceRecordValues(singletonList(createDropDatabase()), secondPoll, db1),
          () -> assertSourceRecordValues(createInserts(101, 150), secondPoll, coll2),
          () -> assertSourceRecordValues(createInserts(1, 48), secondPoll, coll3),
          () -> assertSourceRecordValues(createInserts(101, 150), secondPoll, coll4),
          () ->
              assertFalse(
                  secondPoll.stream()
                      .map(SourceRecord::sourceOffset)
                      .anyMatch(i -> i.containsKey("copy"))));
    }
  }

  @Test
  @DisplayName("Ensure source honours poll max batch size and batch size")
  void testHonoursMaxBatchSize() {
    try (AutoCloseableSourceTask task = createSourceTask(Logger.getLogger(MongoSourceTask.class))) {
      MongoCollection<Document> coll = getAndCreateCollection();
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "25");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "20000");
              put(MongoSourceConfig.BATCH_SIZE_CONFIG, "100");
            }
          };

      task.start(cfg);
      insertMany(rangeClosed(1, 100), coll);
      getNextResults(task);

      Map<String, Map<String, Long>> mBeansMap =
          getMBeanAttributes(
              "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-change-stream-unknown");
      for (Map<String, Long> attrs : mBeansMap.values()) {
        assertEquals(100, attrs.get("records"));
        assertEquals(1, attrs.get("initial-commands-successful"));
        assertWithInRange(IntStream.rangeClosed(2, 4), attrs.get("getmore-commands-successful"));
      }
      task.stop();
    }
  }

  @Test
  @DisplayName("Ensure source can handle non existent database and survive dropping")
  void testSourceCanHandleNonExistentDatabaseAndSurviveDropping() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoDatabase db = getDatabaseWithPostfix();
      MongoCollection<Document> coll1 = db.getCollection("coll1");
      MongoCollection<Document> coll2 = db.getCollection("coll2");
      MongoCollection<Document> coll3 = db.getCollection("coll3");

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, db.getName());
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "150");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
            }
          };
      task.start(cfg);

      assertNull(task.poll());

      insertMany(rangeClosed(1, 50), coll1, coll2);
      insertMany(rangeClosed(101, 150), coll3);

      List<SourceRecord> firstPoll = getNextResults(task);
      assertAll(
          () -> assertSourceRecordValues(createInserts(1, 50), firstPoll, coll1),
          () -> assertSourceRecordValues(createInserts(1, 50), firstPoll, coll2),
          () -> assertSourceRecordValues(createInserts(101, 150), firstPoll, coll3));

      db.drop();

      List<SourceRecord> secondPoll = getNextResults(task);
      assertAll(
          () -> assertSourceRecordValues(singletonList(createDropCollection()), secondPoll, coll1),
          () -> assertSourceRecordValues(singletonList(createDropCollection()), secondPoll, coll2),
          () -> assertSourceRecordValues(singletonList(createDropCollection()), secondPoll, coll3),
          () -> assertSourceRecordValues(singletonList(createDropDatabase()), secondPoll, db));

      assertNull(task.poll());
      insertMany(rangeClosed(51, 100), coll1, coll2, coll3);

      List<SourceRecord> thirdPoll = getNextResults(task);
      assertAll(
          () -> assertSourceRecordValues(createInserts(51, 100), thirdPoll, coll1),
          () -> assertSourceRecordValues(createInserts(51, 100), thirdPoll, coll2),
          () -> assertSourceRecordValues(createInserts(51, 100), thirdPoll, coll3));
    }
  }

  @Test
  @DisplayName("Ensure source can handle non existent database and survive dropping with pipeline")
  void testSourceCanHandleNonExistentDatabaseAndSurviveDroppingWithPipeline() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoDatabase db = getDatabaseWithPostfix();
      MongoCollection<Document> coll1 = db.getCollection("coll1");
      MongoCollection<Document> coll2 = db.getCollection("coll2");
      MongoCollection<Document> coll3 = db.getCollection("coll3");

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, db.getName());
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "150");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
              put(
                  MongoSourceConfig.PIPELINE_CONFIG,
                  "[{\"$match\": {\"operationType\": \"insert\"}}]");
            }
          };
      task.start(cfg);

      assertNull(task.poll());

      insertMany(rangeClosed(1, 50), coll1, coll2);
      insertMany(rangeClosed(101, 150), coll3);

      List<SourceRecord> firstPoll = getNextResults(task);
      assertAll(
          () -> assertSourceRecordValues(createInserts(1, 50), firstPoll, coll1),
          () -> assertSourceRecordValues(createInserts(1, 50), firstPoll, coll2),
          () -> assertSourceRecordValues(createInserts(101, 150), firstPoll, coll3));

      db.drop();
      assertNull(task.poll());
      assertNull(task.poll());
      insertMany(rangeClosed(51, 100), coll1, coll2, coll3);

      List<SourceRecord> secondPoll = getNextResults(task);
      assertAll(
          () -> assertSourceRecordValues(createInserts(51, 100), secondPoll, coll1),
          () -> assertSourceRecordValues(createInserts(51, 100), secondPoll, coll2),
          () -> assertSourceRecordValues(createInserts(51, 100), secondPoll, coll3));
    }
  }

  @Test
  @DisplayName("Ensure source can handle non existent collection and survive dropping")
  void testSourceCanHandleNonExistentCollectionAndSurviveDropping() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = getCollection();

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "50");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
            }
          };

      task.start(cfg);

      assertNull(task.poll());

      insertMany(rangeClosed(1, 50), coll);

      List<SourceRecord> firstPoll = getNextResults(task);
      assertSourceRecordValues(createInserts(1, 50), firstPoll, coll);

      coll.drop();

      List<SourceRecord> secondPoll = getNextResults(task);
      assertSourceRecordValues(singletonList(createDropCollection()), secondPoll, coll);

      assertNull(task.poll());
      insertMany(rangeClosed(51, 100), coll);

      List<SourceRecord> thirdPoll = getNextResults(task);
      assertSourceRecordValues(createInserts(51, 100), thirdPoll, coll);
    }
  }

  @Test
  @DisplayName("Ensure source can handle invalid resume token when error tolerance is set to all")
  void testSourceCanHandleInvalidResumeTokenWhenErrorToleranceIsAll() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = getAndCreateCollection();

      coll.insertOne(Document.parse("{a: 1}"));

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
              put(MongoSourceConfig.BATCH_SIZE_CONFIG, "100");
            }
          };

      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      when(offsetStorageReader.offset(any())).thenReturn(INVALID_OFFSET);
      task.initialize(context);
      task.start(cfg);

      assertNull(task.poll());
      insertMany(rangeClosed(1, 50), coll);

      assertSourceRecordValues(createInserts(1, 50), getNextResults(task), coll);

      if (!isGreaterThanFourDotFour()) {
        Map<String, Map<String, Long>> mBeansMap =
            getMBeanAttributes(
                "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-change-stream-unknown");
        for (Map<String, Long> attrs : mBeansMap.values()) {
          assertEquals(50, attrs.get("records"));
          assertNotEquals(0, attrs.get("mongodb-bytes-read"));
          assertNotEquals(0, attrs.get("initial-commands-successful"));
          assertEquals(3, attrs.get("getmore-commands-successful"));
          assertEquals(1, attrs.get("initial-commands-failed"));
          assertEquals(0, attrs.get("getmore-commands-failed"));
        }
      }
      task.stop();
    }
  }

  @Test
  @DisplayName("Ensure source sets the expected topic mapping")
  void testSourceTopicMapping() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoDatabase db = getDatabaseWithPostfix();
      MongoCollection<Document> coll1 = db.getCollection("coll1");
      MongoCollection<Document> coll2 = db.getCollection("coll2");

      insertMany(rangeClosed(1, 50), coll1, coll2);

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, db.getName());
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "100");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
              put(
                  MongoSourceConfig.TOPIC_NAMESPACE_MAP_CONFIG,
                  format(
                      "{'%s': 'myDB', '%s': 'altDB.altColl'}",
                      db.getName(), coll2.getNamespace().getFullName()));
            }
          };

      task.start(cfg);

      List<SourceRecord> firstPoll = getNextResults(task);
      assertAll(
          () -> assertSourceRecordValues(createInserts(1, 50), firstPoll, "myDB.coll1"),
          () -> assertSourceRecordValues(createInserts(1, 50), firstPoll, "altDB.altColl"));

      insertMany(rangeClosed(51, 100), coll1, coll2);

      List<SourceRecord> secondPoll = getNextResults(task);
      assertAll(
          () -> assertSourceRecordValues(createInserts(51, 100), secondPoll, "myDB.coll1"),
          () -> assertSourceRecordValues(createInserts(51, 100), secondPoll, "altDB.altColl"));
    }
  }

  @Test
  @DisplayName("Ensure source can use custom offset partition names")
  void testSourceCanUseCustomOffsetPartitionNames() {
    assumeTrue(isGreaterThanFourDotZero());
    try (AutoCloseableSourceTask task = createSourceTask(Logger.getLogger(MongoSourceTask.class))) {
      MongoCollection<Document> coll = getAndCreateCollection();

      coll.insertOne(Document.parse("{a: 1}"));

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.OVERRIDE_ERRORS_TOLERANCE_CONFIG, "all");
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "50");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "5000");
              put(MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG, "oldPartitionName");
            }
          };

      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      when(offsetStorageReader.offset(singletonMap("ns", "oldPartitionName")))
          .thenReturn(INVALID_OFFSET);
      task.initialize(context);

      assertDoesNotThrow(
          () -> {
            task.start(cfg);
            getNextBatch(task);
          });

      assertTrue(
          task.logCapture.getEvents().stream()
              .anyMatch(
                  e ->
                      e.getRenderedMessage()
                          .contains("Resuming the change stream after the previous offset")));
      assertTrue(
          task.logCapture.getEvents().stream()
              .anyMatch(e -> e.getRenderedMessage().contains("Failed to resume change stream")));
      task.logCapture.reset();
      task.stop();

      when(offsetStorageReader.offset(singletonMap("ns", "newPartitionName"))).thenReturn(null);
      cfg.put(MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG, "newPartitionName");
      task.start(cfg);

      insertMany(rangeClosed(1, 50), coll);
      assertSourceRecordValues(createInserts(1, 50), getNextResults(task), coll);

      assertTrue(
          task.logCapture.getEvents().stream()
              .anyMatch(
                  e ->
                      e.getRenderedMessage()
                          .contains("New change stream cursor created without offset")));
      task.stop();
    }
  }

  @Test
  @DisplayName("Copy existing with a restart midway through")
  void testCopyingExistingWithARestartMidwayThrough() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoCollection<Document> coll = getCollection();

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "25");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
            }
          };

      insertMany(rangeClosed(1, 50), coll);
      task.start(cfg);

      List<SourceRecord> firstPoll = getNextBatch(task);
      assertSourceRecordValues(createInserts(1, 25), firstPoll, coll);
      assertTrue(
          firstPoll.stream().map(SourceRecord::sourceOffset).allMatch(i -> i.containsKey("copy")));

      Map<String, ?> lastOffset = firstPoll.get(25 - 1).sourceOffset();

      // mock the context so that on restart we know where the last task left off
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      assertNotNull(lastOffset.get("_id")); // check to make sure the value is an Object
      @SuppressWarnings("unchecked")
      Map<String, Object> mockedOffset = (Map<String, Object>) lastOffset;
      when(offsetStorageReader.offset(any())).thenReturn(mockedOffset);
      task.initialize(context);

      // perform a restart
      task.stop();
      task.start(cfg);

      List<SourceRecord> secondPoll = getNextBatch(task);
      assertSourceRecordValues(createInserts(1, 25), secondPoll, coll);
      assertTrue(
          secondPoll.stream().map(SourceRecord::sourceOffset).allMatch(i -> i.containsKey("copy")));

      List<SourceRecord> thirdPoll = getNextBatch(task);
      assertSourceRecordValues(createInserts(26, 50), thirdPoll, coll);
      // Make sure all elements, except the last one, contains the "copy" key
      assertTrue(
          thirdPoll.stream()
              .map(SourceRecord::sourceOffset)
              .limit(25 - 1) // exclude the last record in the batch
              .allMatch(i -> i.containsKey("copy")));
      // Make sure the last copied element does not contain the "copy" key
      assertTrue(
          thirdPoll.stream()
              .map(SourceRecord::sourceOffset)
              .skip(25 - 1) // exclude the last record in the batch
              .findFirst()
              .filter(i -> !i.containsKey("copy"))
              .isPresent());

      assertTrue(getNextBatch(task).isEmpty());
      insertMany(rangeClosed(51, 75), coll);

      List<SourceRecord> fourthPoll = getNextBatch(task);
      assertSourceRecordValues(createInserts(51, 75), fourthPoll, coll);
      assertFalse(
          fourthPoll.stream().map(SourceRecord::sourceOffset).anyMatch(i -> i.containsKey("copy")));
    }
  }

  @Test
  @DisplayName("Copy existing with a restart after finishing")
  void testCopyingExistingWithARestartAfterFinishing() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoCollection<Document> coll = getCollection();

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "25");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
            }
          };

      insertMany(rangeClosed(1, 50), coll);
      task.start(cfg);

      List<SourceRecord> firstPoll = getNextBatch(task);
      assertSourceRecordValues(createInserts(1, 25), firstPoll, coll);
      assertTrue(
          firstPoll.stream().map(SourceRecord::sourceOffset).allMatch(i -> i.containsKey("copy")));

      List<SourceRecord> secondPoll = getNextBatch(task);
      assertSourceRecordValues(createInserts(26, 50), secondPoll, coll);
      // Make sure all elements, except the last one, contains the "copy" key
      assertTrue(
          secondPoll.stream()
              .map(SourceRecord::sourceOffset)
              .limit(25 - 1) // exclude the last record in the batch
              .allMatch(i -> i.containsKey("copy")));

      Map<String, ?> lastOffset = secondPoll.get(25 - 1).sourceOffset();

      // Make sure the last copied element does not contain the "copy" key
      assertFalse(lastOffset.containsKey("copy"));

      // mock the context so that on restart we know where the last task left off
      when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
      assertNotNull(lastOffset.get("_id")); // check to make sure the value is an Object
      @SuppressWarnings("unchecked")
      Map<String, Object> mockedOffset = (Map<String, Object>) lastOffset;
      when(offsetStorageReader.offset(any())).thenReturn(mockedOffset);
      task.initialize(context);

      // perform a restart
      task.stop();
      task.start(cfg);

      // make sure that a copy doesn't occur again because all data was already copied
      assertTrue(getNextBatch(task).isEmpty());

      // make sure that we can continue to process data
      insertMany(rangeClosed(51, 75), coll);

      List<SourceRecord> thirdPoll = getNextBatch(task);
      assertSourceRecordValues(createInserts(51, 75), thirdPoll, coll);
      assertFalse(
          thirdPoll.stream().map(SourceRecord::sourceOffset).anyMatch(i -> i.containsKey("copy")));
    }
  }

  @Test
  @DisplayName("Ensure source loads data from collection and outputs documents only")
  void testSourceLoadsDataFromCollectionDocumentOnly() {
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = getAndCreateCollection();
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
              put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
              put(
                  MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG,
                  "[{\"$match\": {\"myInt\": {\"$gt\": 10}}}]");
              put(MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG, SimplifiedJson.class.getName());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "50");
            }
          };

      String documentString =
          "{'myInt': %s, "
              + "'myString': 'some foo bla text', "
              + "'myDouble': {'$numberDouble': '20.21'}, "
              + "'mySubDoc': {'A': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "  'B': {'$date': {'$numberLong': '1577863627000'}}, 'C': {'$numberDecimal': '12345.6789'}}, "
              + "'myArray': [{'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "  {'$date': {'$numberLong': '1577863627000'}}, {'$numberDecimal': '12345.6789'}], "
              + "'myBytes': {'$binary': {'base64': 'S2Fma2Egcm9ja3Mh', 'subType': '00'}}, "
              + "'myDate': {'$date': {'$numberLong': '1577863627000'}}, "
              + "'myDecimal': {'$numberDecimal': '12345.6789'}}";

      List<Document> docs = insertMany(rangeClosed(1, 60), documentString, coll);
      task.start(cfg);

      JsonWriterSettings settings = new SimplifiedJson().getJsonWriterSettings();
      List<Document> expectedDocs =
          docs.stream()
              .filter(i -> i.get("myInt", 1) > 10)
              .map(d -> Document.parse(d.toJson(settings)))
              .collect(toList());

      List<SourceRecord> poll = getNextResults(task);
      List<Document> actualDocs =
          poll.stream().map(s -> Document.parse(s.value().toString())).collect(toList());
      assertIterableEquals(expectedDocs, actualDocs);
    }
  }

  @Test
  @DisplayName("Test null values are emitted when documents are deleted")
  void testSourceEmitsNullValuesOnDelete() {
    assumeTrue(isGreaterThanFourDotZero());
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = getAndCreateCollection();
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
              put(MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_TOMBSTONE_ON_DELETE_CONFIG, "true");
              put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
              put(
                  MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG,
                  "[{\"$match\": {\"myInt\": {\"$gt\": 10}}}]");
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "50");
            }
          };

      String documentString = "{'myInt': %s}";

      List<Document> docs = insertMany(rangeClosed(1, 60), documentString, coll);
      task.start(cfg);

      List<Document> expectedDocs =
          docs.stream()
              .filter(i -> i.get("myInt", 1) > 10)
              .map(d -> Document.parse(d.toJson()))
              .collect(toList());

      List<SourceRecord> poll = getNextResults(task);
      List<Document> actualDocs =
          poll.stream().map(s -> Document.parse(s.value().toString())).collect(toList());
      assertIterableEquals(expectedDocs, actualDocs);

      coll.deleteMany(new Document());
      List<SourceRecord> pollAfterDelete = getNextResults(task);
      pollAfterDelete.forEach(s -> assertNull(s.value()));

      List<String> documentIds = docs.stream().map(s -> s.get("_id").toString()).collect(toList());
      List<String> connectRecordsKeyIds =
          pollAfterDelete.stream()
              .map(r -> Document.parse(r.key().toString()).get("_id").toString())
              .collect(toList());
      assertIterableEquals(documentIds, connectRecordsKeyIds);
    }
  }

  @Test
  @DisplayName("Ensure source generates heartbeats")
  void testSourceGeneratesHeartbeats() {
    assumeTrue(isGreaterThanThreeDotSix());
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = getCollection();

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG, "heartBeatTopic");
              put(MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "10");
            }
          };

      task.start(cfg);

      insertMany(rangeClosed(1, 10), coll);
      getNextBatch(task).forEach(s -> assertNotEquals("heartBeatTopic", s.topic()));

      getNextBatch(task).forEach(s -> assertEquals("heartBeatTopic", s.topic()));

      insertMany(rangeClosed(11, 20), coll);
      getNextBatch(task).forEach(s -> assertNotEquals("heartBeatTopic", s.topic()));
    }
  }

  @Test
  @DisplayName("Ensure source sends data to the deadletter queue on failures")
  void testDeadletterQueueHandling() {
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = getAndCreateCollection();
      MongoCollection<Document> coll2 = getAndCreateCollection();
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
              put(MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG, SimplifiedJson.class.getName());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "5");
              put(MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.SCHEMA.name());
              put(MongoSourceConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
              put(
                  MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG,
                  "{\"type\" : \"record\", \"name\" : \"fullDocument\","
                      + "\"fields\" : [{\"name\": \"_id\", \"type\": "
                      + "{\"type\" : \"array\", \"items\" : \"int\"}}]}");
            }
          };

      task.start(cfg);

      insertMany(rangeClosed(1, 5), coll);
      assertTrue(getNextResults(task).isEmpty());

      task.stop();

      cfg.put(MongoSourceConfig.DATABASE_CONFIG, coll2.getNamespace().getDatabaseName());
      cfg.put(MongoSourceConfig.COLLECTION_CONFIG, coll2.getNamespace().getCollectionName());
      cfg.put(MongoSourceConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "__dlq");
      task.start(cfg);

      List<Document> docs = insertMany(rangeClosed(1, 5), coll2);
      List<String> expectedDocs = docs.stream().map(Document::toJson).collect(toList());

      List<SourceRecord> poll = getNextResults(task);
      assertTrue(poll.stream().allMatch(s -> s.topic().equals("__dlq")));
      List<String> actualDocs = poll.stream().map(s -> s.value().toString()).collect(toList());
      assertIterableEquals(expectedDocs, actualDocs);
    }
  }

  @Test
  @DisplayName("Ensure source honours error tolerance all")
  void testErrorToleranceAllSupport() {
    try (AutoCloseableSourceTask task = createSourceTask(Logger.getLogger(MongoSourceTask.class))) {
      MongoCollection<Document> coll = getAndCreateCollection();
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
              put(MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG, SimplifiedJson.class.getName());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "10");
              put(MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.SCHEMA.name());
              put(MongoSourceConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
              put(MongoSourceConfig.ERRORS_LOG_ENABLE_CONFIG, "true");
              put(
                  MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG,
                  "{\"type\" : \"record\", \"name\" : \"fullDocument\","
                      + "\"fields\" : [{\"name\": \"_id\", \"type\": \"int\"}]}");
            }
          };

      task.start(cfg);

      Document poisonPill = Document.parse("{_id: {a: 1, b: 2, c: 3}}");
      insertMany(rangeClosed(1, 3), coll);
      coll.insertOne(poisonPill);
      insertMany(rangeClosed(4, 5), coll);

      Schema objectSchema = SchemaBuilder.struct().field("_id", Schema.INT32_SCHEMA).build();
      List<Struct> expectedDocs =
          rangeClosed(1, 5).mapToObj(i -> new Struct(objectSchema).put("_id", i)).collect(toList());

      List<SourceRecord> poll = getNextResults(task);
      poll.addAll(getNextResults(task));

      List<Struct> actualDocs = poll.stream().map(s -> (Struct) s.value()).collect(toList());
      assertStructsEquals(expectedDocs, actualDocs);
      assertTrue(
          task.logCapture.getEvents().stream()
              .filter(e -> e.getLevel().equals(Level.ERROR))
              .anyMatch(
                  e ->
                      e.getMessage().toString().contains("Exception creating Source record for:")));

      // Reset and test copy existing without logs
      task.stop();
      task.logCapture.reset();
      cfg.put(MongoSourceConfig.ERRORS_LOG_ENABLE_CONFIG, "false");
      cfg.put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());

      task.start(cfg);
      poll = getNextResults(task);
      actualDocs = poll.stream().map(s -> (Struct) s.value()).collect(toList());
      assertStructsEquals(expectedDocs, actualDocs);
      assertFalse(
          task.logCapture.getEvents().stream()
              .filter(e -> e.getLevel().equals(Level.ERROR))
              .findFirst()
              .map(e -> e.getMessage().toString())
              .orElseGet(() -> "")
              .contains("Exception creating Source record for:"));
      task.stop();
    }
  }

  @Test
  @DisplayName("Ensure source honours error tolerance none")
  void testErrorToleranceNoneSupport() {
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = getAndCreateCollection();
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG, "true");
              put(MongoSourceConfig.OUTPUT_JSON_FORMATTER_CONFIG, SimplifiedJson.class.getName());
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "5");
              put(MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG, OutputFormat.SCHEMA.name());
              put(
                  MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG,
                  "{\"type\" : \"record\", \"name\" : \"fullDocument\","
                      + "\"fields\" : [{\"name\": \"_id\", \"type\": \"int\"}]}");
            }
          };

      task.start(cfg);

      Document poisonPill = Document.parse("{_id: {a: 1, b: 2, c: 3}}");
      insertMany(rangeClosed(1, 3), coll);
      coll.insertOne(poisonPill);
      insertMany(rangeClosed(4, 5), coll);

      Exception e = assertThrows(DataException.class, () -> getNextResults(task));
      assertTrue(e.getMessage().contains("Exception creating Source record for:"));
    }
  }

  @Test
  @DisplayName("Ensure source honours error tolerance all and > 16mb change stream message")
  void testErrorToleranceAllSupport16MbError() {
    try (AutoCloseableSourceTask task = createSourceTask(Logger.getLogger(MongoSourceTask.class))) {
      MongoCollection<Document> coll = getAndCreateCollection();
      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.FULL_DOCUMENT_CONFIG, "updateLookup");
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "5");
              put(MongoSourceConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
            }
          };

      task.start(cfg);

      insertMany(rangeClosed(1, 5), coll);
      List<SourceRecord> poll = getNextResults(task);
      assertEquals(5, poll.size());

      // Poison the change stream
      coll.updateOne(new Document("_id", 3), Updates.set("y", new byte[(1024 * 1024 * 16) - 30]));
      task.poll(); // Use poll directly as no results are expected.

      // Insert some new data and confirm new events are available post change stream restart
      insertMany(range(10, 15), coll);
      poll = getNextResults(task);

      assertEquals(5, poll.size());
      assertTrue(
          task.logCapture.getEvents().stream()
              .filter(e -> e.getLevel().equals(Level.WARN))
              .anyMatch(
                  e ->
                      e.getMessage()
                          .toString()
                          .startsWith(
                              "Failed to resume change stream: Query failed with error code 10334")));

      Map<String, Map<String, Long>> mBeansMap =
          getMBeanAttributes(
              "com.mongodb.kafka.connect:type=source-task-metrics,connector=MongoSourceConnector,task=source-task-change-stream-unknown");
      for (Map<String, Long> attrs : mBeansMap.values()) {
        assertEquals(10, attrs.get("records"));
        assertNotEquals(0, attrs.get("mongodb-bytes-read"));
        assertEquals(2, attrs.get("initial-commands-successful"));
        assertEquals(4, attrs.get("getmore-commands-successful"));
        assertEquals(0, attrs.get("initial-commands-failed"));
        assertEquals(1, attrs.get("getmore-commands-failed"));
      }
      task.stop();
    }
  }

  @Test
  @DisplayName("Ensure pre-/post-image works")
  void testFullDocumentBeforeChange() {
    assumeTrue(isAtLeastSixDotZero());
    MongoDatabase db = getDatabaseWithPostfix();
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = db.getCollection("coll");
      coll.drop();
      db.createCollection(
          coll.getNamespace().getCollectionName(),
          new CreateCollectionOptions()
              .changeStreamPreAndPostImagesOptions(new ChangeStreamPreAndPostImagesOptions(true)));
      HashMap<String, String> cfg = new HashMap<>();
      cfg.put(
          MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG,
          OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT));
      cfg.put(
          MongoSourceConfig.FULL_DOCUMENT_BEFORE_CHANGE_CONFIG,
          FullDocumentBeforeChange.REQUIRED.getValue());
      cfg.put(MongoSourceConfig.FULL_DOCUMENT_CONFIG, FullDocumentBeforeChange.REQUIRED.getValue());
      task.start(cfg);
      int id = 0;
      Document expected = new Document("_id", id);
      coll.insertOne(expected);
      coll.deleteOne(Filters.eq(id));
      List<SourceRecord> records = getNextResults(task);
      assertEquals(2, records.size());
      Struct insert = (Struct) records.get(0).value();
      assertEquals(OperationType.INSERT.getValue(), insert.getString("operationType"));
      assertEquals(expected.toJson(), insert.getString("fullDocument"));
      Struct delete = (Struct) records.get(1).value();
      assertEquals(OperationType.DELETE.getValue(), delete.getString("operationType"));
      assertEquals(expected.toJson(), delete.getString("fullDocumentBeforeChange"));
    } finally {
      db.drop();
    }
  }

  @Test
  @DisplayName("Ensure disambiguatedPaths exist when showExpandedEvents is true")
  void testDisambiguatedPathsExistWhenShowExpandedEventsIsTrue() {
    assumeTrue(isAtLeastSevenDotZero());
    MongoDatabase db = getDatabaseWithPostfix();
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = db.getCollection("coll");
      coll.drop();
      db.createCollection(coll.getNamespace().getCollectionName(), new CreateCollectionOptions());
      HashMap<String, String> cfg = new HashMap<>();
      cfg.put(
          MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG,
          OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT));
      cfg.put(MongoSourceConfig.SHOW_EXPANDED_EVENTS_CONFIG, "true");
      task.start(cfg);
      int id = 0;
      Document expected = new Document("_id", id);
      coll.insertOne(expected);
      coll.updateOne(Filters.eq(id), Document.parse("{ $set: { foo: 1 } }"));
      coll.deleteOne(Filters.eq(id));
      List<SourceRecord> records = getNextResults(task);
      assertEquals(3, records.size());
      Struct update = (Struct) records.get(1).value();
      assertEquals(OperationType.UPDATE.getValue(), update.getString("operationType"));
      Struct updateDescription = (Struct) update.get("updateDescription");
      assertEquals("{}", updateDescription.getString("disambiguatedPaths"));
    } finally {
      db.drop();
    }
  }

  @Test
  @DisplayName("Ensure disambiguatedPaths don't exist when showExpandedEvents is false")
  void testDisambiguatedPathsDontExistWhenShowExpandedEventsIsTrue() {
    assumeTrue(isAtLeastSevenDotZero());
    MongoDatabase db = getDatabaseWithPostfix();
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = db.getCollection("coll");
      coll.drop();
      db.createCollection(coll.getNamespace().getCollectionName(), new CreateCollectionOptions());
      HashMap<String, String> cfg = new HashMap<>();
      cfg.put(
          MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG,
          OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT));
      cfg.put(MongoSourceConfig.SHOW_EXPANDED_EVENTS_CONFIG, "false");
      task.start(cfg);
      int id = 0;
      Document expected = new Document("_id", id);
      coll.insertOne(expected);
      coll.updateOne(Filters.eq(id), Document.parse("{ $set: { foo: 1 } }"));
      coll.deleteOne(Filters.eq(id));
      List<SourceRecord> records = getNextResults(task);
      assertEquals(3, records.size());
      Struct update = (Struct) records.get(1).value();
      assertEquals(OperationType.UPDATE.getValue(), update.getString("operationType"));
      Struct updateDescription = (Struct) update.get("updateDescription");
      assertNull(updateDescription.getString("disambiguatedPaths"));
    } finally {
      db.drop();
    }
  }

  @Test
  @DisplayName("Ensure disambiguatedPaths don't exist by default")
  void testDisambiguatedPathsDontExistByDefault() {
    assumeTrue(isAtLeastSevenDotZero());
    MongoDatabase db = getDatabaseWithPostfix();
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = db.getCollection("coll");
      coll.drop();
      db.createCollection(coll.getNamespace().getCollectionName(), new CreateCollectionOptions());
      HashMap<String, String> cfg = new HashMap<>();
      cfg.put(
          MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG,
          OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT));
      task.start(cfg);
      int id = 0;
      Document expected = new Document("_id", id);
      coll.insertOne(expected);
      coll.updateOne(Filters.eq(id), Document.parse("{ $set: { foo: 1 } }"));
      coll.deleteOne(Filters.eq(id));
      List<SourceRecord> records = getNextResults(task);
      assertEquals(3, records.size());
      Struct update = (Struct) records.get(1).value();
      assertEquals(OperationType.UPDATE.getValue(), update.getString("operationType"));
      Struct updateDescription = (Struct) update.get("updateDescription");
      assertNull(updateDescription.getString("disambiguatedPaths"));
    } finally {
      db.drop();
    }
  }

  @Test
  @DisplayName("Ensure truncatedArrays works")
  void testTruncatedArrays() {
    assumeTrue(isAtLeastSixDotZero());
    MongoDatabase db = getDatabaseWithPostfix();
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = db.getCollection("coll");
      coll.drop();
      db.createCollection(coll.getNamespace().getCollectionName(), new CreateCollectionOptions());
      HashMap<String, String> cfg = new HashMap<>();
      cfg.put(
          MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG,
          OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT));
      task.start(cfg);
      int id = 0;
      Document expected =
          new Document("_id", id)
              .append("items", Arrays.asList(2, 30, 5, 10, 11, 100, 200, 250, 300, 5, 600));
      coll.insertOne(expected);
      coll.updateOne(
          Filters.eq(id),
          singletonList(Document.parse("{ $set: { items: [2,30,5,10,11,100,200,250,300,5] } }")));
      coll.deleteOne(Filters.eq(id));
      List<SourceRecord> records = getNextResults(task);
      assertEquals(3, records.size());
      Struct update = (Struct) records.get(1).value();
      assertEquals(OperationType.UPDATE.getValue(), update.getString("operationType"));
      Struct updateDescription = (Struct) update.get("updateDescription");

      Schema schema =
          SchemaBuilder.struct()
              .name("truncatedArray")
              .field("field", Schema.STRING_SCHEMA)
              .field("newSize", Schema.INT32_SCHEMA)
              .build();

      Struct truncatedArrayStruct = new Struct(schema).put("field", "items").put("newSize", 10);

      List<Struct> expectedTruncatedArray = new ArrayList<>();
      expectedTruncatedArray.add(truncatedArrayStruct);
      assertEquals(expectedTruncatedArray, updateDescription.getArray("truncatedArrays"));
    } finally {
      db.drop();
    }
  }

  /**
   * We insert a document into a collection before starting the {@link MongoSourceTask}, yet we
   * observe the change due to specifying {@link
   * MongoSourceConfig#STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG}.
   */
  @Test
  void testStartAtOperationTime() {
    assumeTrue(isGreaterThanFourDotZero());
    MongoDatabase db = getDatabaseWithPostfix();
    try (AutoCloseableSourceTask task = createSourceTask()) {
      MongoCollection<Document> coll = db.getCollection("coll");
      coll.drop();
      int id = 0;
      Document expected = new Document("_id", id);
      coll.insertOne(expected);
      HashMap<String, String> cfg = new HashMap<>();
      cfg.put(MongoSourceConfig.STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue());
      cfg.put(
          MongoSourceConfig.STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG,
          Instant.EPOCH.toString());
      cfg.put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
      cfg.put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
      cfg.put(
          MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG,
          OutputFormat.SCHEMA.name().toLowerCase(Locale.ROOT));
      task.start(cfg);
      List<Struct> records =
          getNextBatch(task).stream()
              .map(r -> (Struct) (r.value()))
              // filter out `drop` and `create` collection events
              .filter(r -> r.getString("operationType").equals(OperationType.INSERT.getValue()))
              .collect(toList());
      assertEquals(1, records.size());
      Struct insert = records.get(0);
      assertEquals(expected.toJson(), insert.getString("fullDocument"));
    } finally {
      db.drop();
    }
  }

  private void assertWithInRange(final IntStream intStream, final long actual) {
    assertTrue(intStream.anyMatch(i -> i == actual));
  }

  private void assertSourceRecordValues(
      final List<? extends ChangeStreamOperation> expectedChangeStreamOperations,
      final List<SourceRecord> allSourceRecords,
      final MongoCollection<?> coll) {
    assertSourceRecordValues(
        expectedChangeStreamOperations, allSourceRecords, coll.getNamespace().getFullName());
  }

  private void assertSourceRecordValues(
      final List<? extends ChangeStreamOperation> expectedChangeStreamOperations,
      final List<SourceRecord> allSourceRecords,
      final MongoDatabase db) {
    assertSourceRecordValues(expectedChangeStreamOperations, allSourceRecords, db.getName());
  }

  private void assertSourceRecordValues(
      final List<? extends ChangeStreamOperation> expectedChangeStreamOperations,
      final List<SourceRecord> allSourceRecords,
      final String topicSuffix) {
    List<ChangeStreamOperation> actualChangeStreamOperations =
        allSourceRecords.stream()
            .filter(s -> s.topic().endsWith(topicSuffix))
            .map(i -> createChangeStreamOperation(Document.parse(i.value().toString())))
            .collect(toList());

    assertIterableEquals(expectedChangeStreamOperations, actualChangeStreamOperations);
  }

  public List<SourceRecord> getNextResults(final AutoCloseableSourceTask task) {
    List<SourceRecord> sourceRecords = new ArrayList<>();
    List<SourceRecord> current;
    do {
      current = task.poll();
      if (current != null) {
        sourceRecords.addAll(current);
      }
    } while (current != null && !current.isEmpty());
    return sourceRecords;
  }

  public List<SourceRecord> getNextBatch(final AutoCloseableSourceTask task) {
    List<SourceRecord> current;
    for (int i = 0; i < 5; i++) {
      current = task.poll();
      if (current != null) {
        return current;
      }
    }
    return emptyList();
  }

  public AutoCloseableSourceTask createSourceTask() {
    return new AutoCloseableSourceTask(new MongoSourceTask());
  }

  public AutoCloseableSourceTask createSourceTask(final Logger logger) {
    return new AutoCloseableSourceTask(new MongoSourceTask(), logger);
  }

  static class AutoCloseableSourceTask extends SourceTask implements AutoCloseable {

    private final MongoSourceTask wrapped;
    private final LogCapture logCapture;

    AutoCloseableSourceTask(final MongoSourceTask wrapped) {
      this(wrapped, null);
    }

    AutoCloseableSourceTask(final MongoSourceTask wrapped, final Logger logger) {
      this.wrapped = wrapped;
      this.logCapture = logger != null ? new LogCapture(logger) : null;
    }

    @Override
    public void initialize(final SourceTaskContext context) {
      wrapped.initialize(context);
    }

    @Override
    public void close() {
      wrapped.stop();
      if (logCapture != null) {
        logCapture.close();
      }
    }

    @Override
    public String version() {
      return wrapped.version();
    }

    @Override
    public void start(final Map<String, String> overrides) {
      HashMap<String, String> props = new HashMap<>();
      props.put(MongoSourceConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
      props.putAll(overrides);
      wrapped.start(props);
    }

    @Override
    public List<SourceRecord> poll() {
      return wrapped.poll();
    }

    @Override
    public void stop() {
      wrapped.stop();
    }
  }
}
