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
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.mongodb.ChangeStreamOperations.ChangeStreamOperation;
import com.mongodb.kafka.connect.mongodb.MongoKafkaTestCase;
import com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson;

public class MongoSourceTaskTest extends MongoKafkaTestCase {

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

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "150");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
            }
          };
      task.start(cfg);

      insertMany(rangeClosed(1, 75), coll1, coll2);

      List<SourceRecord> firstPoll = getNextResults(task);

      assertAll(
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll1),
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll2));

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
              put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "150");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "1000");
            }
          };
      task.start(cfg);
      List<SourceRecord> firstPoll = getNextResults(task);

      assertAll(
          () -> assertEquals(150, firstPoll.size()),
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll1),
          () -> assertSourceRecordValues(createInserts(1, 75), firstPoll, coll2),
          () ->
              assertTrue(
                  firstPoll.stream()
                      .map(SourceRecord::sourceOffset)
                      .allMatch(i -> i.containsKey("copy"))));

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
              put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
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
              put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
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
              put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
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
  @DisplayName("Copy existing with a restart midway through")
  void testCopyingExistingWithARestartMidwayThrough() {
    try (AutoCloseableSourceTask task = createSourceTask()) {

      MongoCollection<Document> coll = getCollection();

      HashMap<String, String> cfg =
          new HashMap<String, String>() {
            {
              put(MongoSourceConfig.DATABASE_CONFIG, coll.getNamespace().getDatabaseName());
              put(MongoSourceConfig.COLLECTION_CONFIG, coll.getNamespace().getCollectionName());
              put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
              put(MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG, "25");
              put(MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG, "2000");
            }
          };

      insertMany(rangeClosed(1, 50), coll);
      task.start(cfg);

      List<SourceRecord> firstPoll = getNextResults(task);
      assertSourceRecordValues(createInserts(1, 25), firstPoll, coll);
      assertTrue(
          firstPoll.stream().map(SourceRecord::sourceOffset).allMatch(i -> i.containsKey("copy")));

      task.stop();
      task.start(cfg);

      List<SourceRecord> secondPoll = getNextResults(task);
      assertSourceRecordValues(createInserts(1, 25), secondPoll, coll);
      assertTrue(
          secondPoll.stream().map(SourceRecord::sourceOffset).allMatch(i -> i.containsKey("copy")));

      List<SourceRecord> thirdPoll = getNextResults(task);
      assertSourceRecordValues(createInserts(26, 50), thirdPoll, coll);
      assertTrue(
          thirdPoll.stream().map(SourceRecord::sourceOffset).allMatch(i -> i.containsKey("copy")));

      assertNull(task.poll());
      insertMany(rangeClosed(51, 75), coll);

      List<SourceRecord> fourthPoll = getNextResults(task);
      assertSourceRecordValues(createInserts(51, 75), fourthPoll, coll);
      assertFalse(
          fourthPoll.stream().map(SourceRecord::sourceOffset).anyMatch(i -> i.containsKey("copy")));
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
              put(MongoSourceConfig.COPY_EXISTING_CONFIG, "true");
              put(
                  MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG,
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
    int counter = 0;
    while (counter < 5) {
      counter++;
      List<SourceRecord> results = task.poll();
      if (results != null) {
        return results;
      }
    }
    throw new DataException("Returned no results");
  }

  public AutoCloseableSourceTask createSourceTask() {
    return new AutoCloseableSourceTask(new MongoSourceTask());
  }

  static class AutoCloseableSourceTask extends SourceTask implements AutoCloseable {

    private final MongoSourceTask wrapped;

    AutoCloseableSourceTask(final MongoSourceTask wrapped) {
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
      props.put(MongoSourceConfig.CONNECTION_URI_CONFIG, MONGODB.getConnectionString().toString());
      overrides.forEach(props::put);
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
