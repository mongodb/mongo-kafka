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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_DATABASE;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.internal.stubbing.defaultanswers.ReturnsSmartNulls;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTask.StartedMongoSinkTask;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ErrorTolerance;

import com.google.common.base.Functions;

final class StartedMongoSinkTaskTest {
  private static final String TEST_TOPIC2 = "topic2";
  private static final MongoNamespace DEFAULT_NAMESPACE =
      new MongoNamespace(TEST_DATABASE, "myColl");

  private Map<String, String> properties;
  private FakeClient client;
  private FakeErrorReporter errorReporter;

  @BeforeEach
  void setUp() {
    properties = new HashMap<>();
    properties.put(TOPICS_CONFIG, TEST_TOPIC + "," + TEST_TOPIC2);
    properties.put(DATABASE_CONFIG, TEST_DATABASE);
    properties.put(COLLECTION_CONFIG, DEFAULT_NAMESPACE.getCollectionName());
    client = new FakeClient();
    errorReporter = new FakeErrorReporter();
  }

  @Test
  void put() {
    MongoSinkConfig config = new MongoSinkConfig(properties);
    client.configureCapturing(DEFAULT_NAMESPACE);
    StartedMongoSinkTask task =
        new StartedMongoSinkTask(config, client.mongoClient(), errorReporter);
    RecordsAndExpectations recordsAndExpectations =
        new RecordsAndExpectations(
            asList(
                Records.simpleValid(TEST_TOPIC, 0),
                Records.simpleValid(TEST_TOPIC, 1),
                Records.simpleValid(TEST_TOPIC2, 2)),
            asList(0, 1, 2),
            emptyList());
    task.put(recordsAndExpectations.records());
    recordsAndExpectations.assertExpectations(
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE),
        errorReporter.reported(),
        Records::match);
  }

  @Test
  void putTolerateAllPostProcessingError() {
    properties.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
    MongoSinkConfig config = new MongoSinkConfig(properties);
    client.configureCapturing(DEFAULT_NAMESPACE);
    StartedMongoSinkTask task =
        new StartedMongoSinkTask(config, client.mongoClient(), errorReporter);
    RecordsAndExpectations recordsAndExpectations =
        new RecordsAndExpectations(
            asList(
                Records.simpleValid(TEST_TOPIC, 0),
                Records.simpleInvalid(TEST_TOPIC, 1),
                Records.simpleValid(TEST_TOPIC, 2),
                Records.simpleValid(TEST_TOPIC2, 3)),
            asList(0, 2, 3),
            singletonList(new Report(1, DataException.class)));
    task.put(recordsAndExpectations.records());
    recordsAndExpectations.assertExpectations(
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE),
        errorReporter.reported(),
        Records::match);
  }

  @Test
  void putTolerateNonePostProcessingError() {
    properties.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.NONE.value());
    MongoSinkConfig config = new MongoSinkConfig(properties);
    client.configureCapturing(DEFAULT_NAMESPACE);
    StartedMongoSinkTask task =
        new StartedMongoSinkTask(config, client.mongoClient(), errorReporter);
    RecordsAndExpectations recordsAndExpectations =
        new RecordsAndExpectations(
            asList(Records.simpleValid(TEST_TOPIC, 0), Records.simpleInvalid(TEST_TOPIC, 1)),
            emptyList(),
            emptyList());
    assertThrows(RuntimeException.class, () -> task.put(recordsAndExpectations.records()));
    recordsAndExpectations.assertExpectations(
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE),
        errorReporter.reported(),
        Records::match);
  }

  @Test
  void putTolerateAllOrderedWriteError() {
    properties.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
    MongoSinkConfig config = new MongoSinkConfig(properties);
    client.configureCapturing(
        DEFAULT_NAMESPACE,
        collection ->
            when(collection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                // batch1
                .thenReturn(BulkWriteResult.unacknowledged())
                // batch2
                .thenThrow(bulkWriteException(singletonList(2), false))
                // batch2
                .thenReturn(BulkWriteResult.unacknowledged()));
    StartedMongoSinkTask task =
        new StartedMongoSinkTask(config, client.mongoClient(), errorReporter);
    RecordsAndExpectations recordsAndExpectations =
        new RecordsAndExpectations(
            asList(
                // batch1
                Records.simpleValid(TEST_TOPIC, 0),
                // batch2
                Records.simpleValid(TEST_TOPIC2, 1),
                Records.simpleValid(TEST_TOPIC2, 2),
                Records.simpleValid(TEST_TOPIC2, 3),
                // batch3
                Records.simpleValid(TEST_TOPIC, 4)),
            asList(0, 1, 2, 3, 4),
            emptyList());
    task.put(recordsAndExpectations.records());
    recordsAndExpectations.assertExpectations(
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE),
        errorReporter.reported(),
        Records::match);
  }

  @SuppressWarnings("unchecked")
  private static <T> T cast(final Object o) {
    return (T) o;
  }

  private static MongoBulkWriteException bulkWriteException(
      final List<Integer> writeErrorIndices, final boolean writeConcernError) {
    List<BulkWriteError> writeErrors =
        writeErrorIndices.stream()
            .map(idx -> new BulkWriteError(0, "", BsonDocument.parse("{}"), idx))
            .collect(Collectors.toList());
    return new MongoBulkWriteException(
        BulkWriteResult.unacknowledged(),
        writeErrors,
        writeConcernError ? new WriteConcernError(0, "", "", BsonDocument.parse("{}")) : null,
        new ServerAddress(),
        emptySet());
  }

  private static final class Records {
    static SinkRecord simpleValid(final String topicName, final int idx) {
      return new SinkRecord(
          topicName,
          0,
          Schema.STRING_SCHEMA,
          "",
          Schema.STRING_SCHEMA,
          String.format("{\"_id\": %d}", idx),
          idx);
    }

    static SinkRecord simpleInvalid(final String topicName, final int idx) {
      return new SinkRecord(
          topicName, 0, Schema.STRING_SCHEMA, "", Schema.STRING_SCHEMA, "notJSON_" + idx, idx);
    }

    static boolean match(
        final SinkRecord expected, final WriteModel<? extends BsonDocument> actual) {
      ReplaceOneModel<BsonDocument> writeModel = cast(actual);
      return expected.value().equals(writeModel.getReplacement().toJson());
    }
  }

  private static final class FakeErrorReporter implements Consumer<MongoProcessedSinkRecordData> {
    private final List<MongoProcessedSinkRecordData> reported = new ArrayList<>();

    @Override
    public void accept(final MongoProcessedSinkRecordData mongoProcessedSinkRecordData) {
      reported.add(mongoProcessedSinkRecordData);
    }

    List<MongoProcessedSinkRecordData> reported() {
      return unmodifiableList(reported);
    }
  }

  private static final class FakeClient {
    private final MongoClient client;
    private final Map<String, MongoDatabase> dbs;
    private final Set<MongoNamespace> configuredNamespaces;

    FakeClient() {
      client = mock(MongoClient.class);
      dbs = new HashMap<>();
      configuredNamespaces = new HashSet<>();
    }

    MongoClient mongoClient() {
      return client;
    }

    void configureCapturing(final MongoNamespace namespace) {
      configureCapturing(namespace, collection -> {});
    }

    void configureCapturing(
        final MongoNamespace namespace, final Consumer<MongoCollection<BsonDocument>> tuner) {
      if (configuredNamespaces.contains(namespace)) {
        return;
      }
      String dbName = namespace.getDatabaseName();
      ReturnsSmartNulls defaultAnswer = new ReturnsSmartNulls();
      MongoDatabase db =
          dbs.computeIfAbsent(
              dbName,
              name -> {
                MongoDatabase newDb = mock(MongoDatabase.class, defaultAnswer);
                when(client.getDatabase(eq(name))).thenReturn(newDb);
                return newDb;
              });
      MongoCollection<BsonDocument> collection = cast(mock(MongoCollection.class, defaultAnswer));
      when(db.getCollection(eq(namespace.getCollectionName()), eq(BsonDocument.class)))
          .thenReturn(collection);
      tuner.accept(collection);
      configuredNamespaces.add(namespace);
    }

    Map<MongoNamespace, List<CapturedBulkWrite>> capturedBulkWrites() {
      return configuredNamespaces.stream()
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  namespace -> {
                    MongoCollection<BsonDocument> collection =
                        client
                            .getDatabase(namespace.getDatabaseName())
                            .getCollection(namespace.getCollectionName(), BsonDocument.class);
                    ArgumentCaptor<List<? extends WriteModel<? extends BsonDocument>>>
                        writeModelsCaptor = cast(ArgumentCaptor.forClass(List.class));
                    verify(collection, atLeast(0)).bulkWrite(writeModelsCaptor.capture(), any());
                    return writeModelsCaptor.getAllValues().stream()
                        .map(CapturedBulkWrite::new)
                        .collect(Collectors.toList());
                  }));
    }

    static final class CapturedBulkWrite {
      private final List<? extends WriteModel<? extends BsonDocument>> models;

      CapturedBulkWrite(final List<? extends WriteModel<? extends BsonDocument>> models) {
        this.models = unmodifiableList(models);
      }

      List<? extends WriteModel<? extends BsonDocument>> models() {
        return models;
      }
    }
  }

  private static final class RecordsAndExpectations {
    private final List<SinkRecord> records;
    private final Set<Integer> expectedWriteAttemptedIndices;
    private final Map<Integer, Report> expectedReports;

    /** Indices must be consecutive and start with 0. */
    RecordsAndExpectations(
        final List<SinkRecord> records,
        final List<Integer> expectedWriteAttemptedIndices,
        final List<Report> expectedReports) {
      this.records = unmodifiableList(records);
      this.expectedWriteAttemptedIndices = new HashSet<>(expectedWriteAttemptedIndices);
      this.expectedReports =
          expectedReports.stream().collect(Collectors.toMap(Report::idx, Functions.identity()));
      int maxValidIndex = records.size() - 1;
      assertTrue(expectedWriteAttemptedIndices.stream().min(Integer::compareTo).orElse(0) >= 0);
      assertTrue(
          expectedWriteAttemptedIndices.stream().max(Integer::compareTo).orElse(maxValidIndex)
              <= maxValidIndex);
      assertTrue(this.expectedReports.keySet().stream().min(Integer::compareTo).orElse(0) >= 0);
      assertTrue(
          this.expectedReports.keySet().stream().max(Integer::compareTo).orElse(maxValidIndex)
              <= maxValidIndex);
    }

    List<SinkRecord> records() {
      return records;
    }

    void assertExpectations(
        final List<FakeClient.CapturedBulkWrite> actualBulkWrites,
        final List<MongoProcessedSinkRecordData> actualReported,
        final BiPredicate<SinkRecord, WriteModel<? extends BsonDocument>> writeModelMatcher) {
      LinkedList<WriteModel<? extends BsonDocument>> writeModels =
          actualBulkWrites.stream()
              .flatMap(capturedBulkWrite -> capturedBulkWrite.models().stream())
              .collect(Collectors.toCollection(LinkedList::new));
      LinkedList<MongoProcessedSinkRecordData> reported = new LinkedList<>(actualReported);
      for (int i = 0; i < records.size(); i++) {
        SinkRecord record = records.get(i);
        if (expectedWriteAttemptedIndices.contains(i)) {
          assertFalse(
              writeModels.isEmpty(),
              String.format("Record index %d. Did not attempt to write %s", i, record));
          WriteModel<? extends BsonDocument> writeModel = writeModels.poll();
          assertTrue(
              writeModelMatcher.test(record, writeModel),
              String.format(
                  "Record index %d. Expected %s does not match  actual %s, ",
                  i, record, writeModel));
        } else {
          if (!writeModels.isEmpty()) {
            WriteModel<? extends BsonDocument> writeModel = writeModels.peek();
            assertFalse(
                writeModelMatcher.test(record, writeModel),
                String.format("Record index %d. Attempted to write %s", i, record));
          }
        }
        if (expectedReports.containsKey(i)) {
          assertFalse(
              reported.isEmpty(), String.format("Record index %d. Did not report %s", i, record));
          MongoProcessedSinkRecordData reportedData = reported.poll();
          SinkRecord reportedRecord = reportedData.getSinkRecord();
          assertEquals(record, reportedRecord);
          Class<? extends Exception> expectedException = expectedReports.get(i).exceptionClass();
          Exception reportedException = reportedData.getException();
          assertNotNull(
              reportedException,
              String.format(
                  "Record index %d. Expected %s but no exception was reported",
                  i, expectedException));
          assertTrue(
              expectedException.isAssignableFrom(reportedException.getClass()),
              String.format(
                  "Record index %d. Expected %s, actual %s, ",
                  i, expectedException, reportedException));
        } else {
          if (!reported.isEmpty()) {
            SinkRecord reportedRecord = reported.peek().getSinkRecord();
            assertNotEquals(record, reportedRecord);
          }
        }
      }
    }
  }

  private static final class Report {
    private final int idx;
    private final Class<? extends Exception> exceptionClass;

    Report(final int idx, final Class<? extends Exception> exceptionClass) {
      this.idx = idx;
      this.exceptionClass = exceptionClass;
    }

    int idx() {
      return idx;
    }

    Class<? extends Exception> exceptionClass() {
      return exceptionClass;
    }
  }
}
