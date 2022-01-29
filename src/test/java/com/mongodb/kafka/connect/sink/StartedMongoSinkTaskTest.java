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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.BULK_WRITE_ORDERED_DEFAULT;
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
import static org.junit.jupiter.api.Assertions.assertAll;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
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

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ErrorTolerance;
import com.mongodb.kafka.connect.sink.dlq.ErrorReporter;
import com.mongodb.kafka.connect.sink.dlq.WriteConcernException;
import com.mongodb.kafka.connect.sink.dlq.WriteException;
import com.mongodb.kafka.connect.sink.dlq.WriteSkippedException;

import com.google.common.base.Functions;

final class StartedMongoSinkTaskTest {
  private static final String TEST_TOPIC2 = "topic2";
  private static final MongoNamespace DEFAULT_NAMESPACE =
      new MongoNamespace(TEST_DATABASE, "myColl");

  private Map<String, String> properties;
  private BulkWritesCapturingClient client;
  private InMemoryErrorReporter errorReporter;

  @BeforeEach
  void setUp() {
    properties = new HashMap<>();
    properties.put(TOPICS_CONFIG, TEST_TOPIC + "," + TEST_TOPIC2);
    properties.put(DATABASE_CONFIG, TEST_DATABASE);
    properties.put(COLLECTION_CONFIG, DEFAULT_NAMESPACE.getCollectionName());
    client = new BulkWritesCapturingClient();
    errorReporter = new InMemoryErrorReporter();
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
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE), errorReporter.reported());
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
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE), errorReporter.reported());
  }

  @Test
  void putTolerateNonePostProcessingError() {
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
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE), errorReporter.reported());
  }

  @Test
  void putTolerateNoneWriteError() {
    MongoSinkConfig config = new MongoSinkConfig(properties);
    client.configureCapturing(
        DEFAULT_NAMESPACE,
        collection ->
            when(collection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                // batch1
                .thenThrow(bulkWriteException(emptyList(), true))
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
                Records.simpleValid(TEST_TOPIC2, 1)),
            singletonList(0),
            emptyList());
    assertThrows(DataException.class, () -> task.put(recordsAndExpectations.records()));
    recordsAndExpectations.assertExpectations(
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE), errorReporter.reported());
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
                .thenThrow(bulkWriteException(singletonList(0), false))
                // batch3
                .thenThrow(bulkWriteException(emptyList(), true))
                // batch4
                .thenThrow(bulkWriteException(singletonList(1), true)));
    StartedMongoSinkTask task =
        new StartedMongoSinkTask(config, client.mongoClient(), errorReporter);
    RecordsAndExpectations recordsAndExpectations =
        new RecordsAndExpectations(
            asList(
                // batch1
                Records.simpleValid(TEST_TOPIC, 0),
                Records.simpleValid(TEST_TOPIC, 1),
                // batch2
                Records.simpleValid(TEST_TOPIC2, 2),
                Records.simpleValid(TEST_TOPIC2, 3),
                Records.simpleValid(TEST_TOPIC2, 4),
                // batch3
                Records.simpleValid(TEST_TOPIC, 5),
                Records.simpleValid(TEST_TOPIC, 6),
                // batch4
                Records.simpleValid(TEST_TOPIC2, 7),
                Records.simpleValid(TEST_TOPIC2, 8),
                Records.simpleValid(TEST_TOPIC2, 9)),
            asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            asList(
                // batch2
                new Report(2, WriteException.class),
                new Report(3, WriteSkippedException.class),
                new Report(4, WriteSkippedException.class),
                // batch3
                new Report(5, WriteConcernException.class),
                new Report(6, WriteConcernException.class),
                // batch4
                new Report(7, WriteConcernException.class),
                new Report(8, WriteException.class),
                new Report(9, WriteSkippedException.class)));
    task.put(recordsAndExpectations.records());
    recordsAndExpectations.assertExpectations(
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE), errorReporter.reported());
  }

  @Test
  void putTolerateAllUnorderedWriteError() {
    properties.put(MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value());
    boolean bulkWriteOrdered = false;
    properties.put(
        MongoSinkTopicConfig.BULK_WRITE_ORDERED_CONFIG, String.valueOf(bulkWriteOrdered));
    MongoSinkConfig config = new MongoSinkConfig(properties);
    client.configureCapturing(
        DEFAULT_NAMESPACE,
        collection ->
            when(collection.bulkWrite(anyList(), any(BulkWriteOptions.class)))
                // batch1
                .thenReturn(BulkWriteResult.unacknowledged())
                // batch2
                .thenThrow(bulkWriteException(asList(0, 2), false))
                // batch3
                .thenThrow(bulkWriteException(emptyList(), true))
                // batch4
                .thenThrow(bulkWriteException(singletonList(1), true)));
    StartedMongoSinkTask task =
        new StartedMongoSinkTask(config, client.mongoClient(), errorReporter);
    RecordsAndExpectations recordsAndExpectations =
        new RecordsAndExpectations(
            asList(
                // batch1
                Records.simpleValid(TEST_TOPIC, 0),
                Records.simpleValid(TEST_TOPIC, 1),
                // batch2
                Records.simpleValid(TEST_TOPIC2, 2),
                Records.simpleValid(TEST_TOPIC2, 3),
                Records.simpleValid(TEST_TOPIC2, 4),
                // batch3
                Records.simpleValid(TEST_TOPIC, 5),
                Records.simpleValid(TEST_TOPIC, 6),
                // batch4
                Records.simpleValid(TEST_TOPIC2, 7),
                Records.simpleValid(TEST_TOPIC2, 8),
                Records.simpleValid(TEST_TOPIC2, 9)),
            asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
            asList(
                // batch2
                new Report(2, WriteException.class),
                new Report(4, WriteException.class),
                // batch3
                new Report(5, WriteConcernException.class),
                new Report(6, WriteConcernException.class),
                // batch4
                new Report(7, WriteConcernException.class),
                new Report(8, WriteException.class),
                new Report(9, WriteConcernException.class)),
            BulkWriteOpts.newDefault().ordered(bulkWriteOrdered));
    task.put(recordsAndExpectations.records());
    recordsAndExpectations.assertExpectations(
        client.capturedBulkWrites().get(DEFAULT_NAMESPACE), errorReporter.reported());
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

  private static final class BulkWriteOpts {
    static BulkWriteOptions newDefault() {
      return new BulkWriteOptions().ordered(BULK_WRITE_ORDERED_DEFAULT);
    }

    static void assertEquals(final BulkWriteOptions expected, final BulkWriteOptions actual) {
      assertAll(
          String.format("expected: %s but was: %s", expected, actual),
          () -> Assertions.assertEquals(expected.isOrdered(), actual.isOrdered()),
          () ->
              Assertions.assertEquals(
                  expected.getBypassDocumentValidation(), actual.getBypassDocumentValidation()));
    }
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
      assertTrue(actual instanceof ReplaceOneModel);
      ReplaceOneModel<BsonDocument> writeModel = cast(actual);
      return expected.value().equals(writeModel.getReplacement().toJson());
    }
  }

  private static final class InMemoryErrorReporter implements ErrorReporter {
    private final List<ReportedData> reported = new ArrayList<>();

    @Override
    public void report(final SinkRecord record, final Exception e) {
      reported.add(new ReportedData(record, e));
    }

    List<ReportedData> reported() {
      return unmodifiableList(reported);
    }

    static final class ReportedData {
      private final SinkRecord record;
      private final Throwable e;

      private ReportedData(final SinkRecord record, final Throwable e) {
        this.record = record;
        this.e = e;
      }

      SinkRecord record() {
        return record;
      }

      Throwable exception() {
        return e;
      }

      @Override
      public String toString() {
        return "ReportedData{" + "record=" + record + ", e=" + e + '}';
      }
    }
  }

  private static final class BulkWritesCapturingClient {
    private final MongoClient client;
    private final Map<String, MongoDatabase> dbs;
    private final Set<MongoNamespace> configuredNamespaces;

    BulkWritesCapturingClient() {
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
                        modelsCaptor = cast(ArgumentCaptor.forClass(List.class));
                    ArgumentCaptor<BulkWriteOptions> optionsCaptor =
                        cast(ArgumentCaptor.forClass(BulkWriteOptions.class));
                    verify(collection, atLeast(0))
                        .bulkWrite(modelsCaptor.capture(), optionsCaptor.capture());
                    List<List<? extends WriteModel<? extends BsonDocument>>> models =
                        modelsCaptor.getAllValues();
                    List<BulkWriteOptions> options = optionsCaptor.getAllValues();
                    assertEquals(models.size(), options.size());
                    int bulkWritesCount = options.size();
                    return IntStream.range(0, bulkWritesCount)
                        .mapToObj(i -> new CapturedBulkWrite(models.get(i), options.get(i)))
                        .collect(Collectors.toList());
                  }));
    }

    static final class CapturedBulkWrite {
      private final List<? extends WriteModel<? extends BsonDocument>> models;
      private final BulkWriteOptions options;

      CapturedBulkWrite(
          final List<? extends WriteModel<? extends BsonDocument>> models,
          final BulkWriteOptions options) {
        this.models = unmodifiableList(models);
        this.options = options;
      }

      List<? extends WriteModel<? extends BsonDocument>> models() {
        return models;
      }

      BulkWriteOptions options() {
        return options;
      }
    }
  }

  private static final class RecordsAndExpectations {
    private final List<SinkRecord> records;
    private final Set<Integer> expectedWriteAttemptedIndices;
    private final Map<Integer, Report> expectedReports;
    private final BulkWriteOptions expectedWriteOptions;

    /** @see RecordsAndExpectations#RecordsAndExpectations(List, List, List, BulkWriteOptions) */
    RecordsAndExpectations(
        final List<SinkRecord> records,
        final List<Integer> expectedWriteAttemptedIndices,
        final List<Report> expectedReports) {
      this(records, expectedWriteAttemptedIndices, expectedReports, BulkWriteOpts.newDefault());
    }

    /** @param expectedWriteAttemptedIndices indices must be consecutive and start with 0. */
    RecordsAndExpectations(
        final List<SinkRecord> records,
        final List<Integer> expectedWriteAttemptedIndices,
        final List<Report> expectedReports,
        final BulkWriteOptions expectedWriteOptions) {
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
      this.expectedWriteOptions = expectedWriteOptions;
    }

    List<SinkRecord> records() {
      return records;
    }

    void assertExpectations(
        final List<BulkWritesCapturingClient.CapturedBulkWrite> actualBulkWrites,
        final List<InMemoryErrorReporter.ReportedData> actualReported) {
      actualBulkWrites.stream()
          .map(BulkWritesCapturingClient.CapturedBulkWrite::options)
          .forEach(writeOptions -> BulkWriteOpts.assertEquals(expectedWriteOptions, writeOptions));
      LinkedList<WriteModel<? extends BsonDocument>> writeModels =
          actualBulkWrites.stream()
              .flatMap(capturedBulkWrite -> capturedBulkWrite.models().stream())
              .collect(Collectors.toCollection(LinkedList::new));
      LinkedList<InMemoryErrorReporter.ReportedData> reported = new LinkedList<>(actualReported);
      for (int i = 0; i < records.size(); i++) {
        SinkRecord record = records.get(i);
        if (expectedWriteAttemptedIndices.contains(i)) {
          assertFalse(
              writeModels.isEmpty(),
              String.format("Record index %d. Did not attempt to write %s", i, record));
          WriteModel<? extends BsonDocument> writeModel = writeModels.poll();
          assertTrue(
              Records.match(record, writeModel),
              String.format(
                  "Record index %d. Expected %s does not match actual %s", i, record, writeModel));
        } else {
          if (!writeModels.isEmpty()) {
            WriteModel<? extends BsonDocument> writeModel = writeModels.peek();
            assertFalse(
                Records.match(record, writeModel),
                String.format("Record index %d. Attempted to write %s", i, record));
          }
        }
        if (expectedReports.containsKey(i)) {
          assertFalse(
              reported.isEmpty(), String.format("Record index %d. Did not report %s", i, record));
          InMemoryErrorReporter.ReportedData reportedData = reported.poll();
          SinkRecord reportedRecord = reportedData.record();
          assertEquals(
              record,
              reportedRecord,
              String.format("Record index %d. Expected %s, actual %s", i, record, reportedRecord));
          Class<? extends Exception> expectedException = expectedReports.get(i).exceptionClass();
          Throwable reportedException = reportedData.exception();
          assertNotNull(
              reportedException,
              String.format(
                  "Record index %d. Expected %s but no exception was reported",
                  i, expectedException));
          assertTrue(
              expectedException.isAssignableFrom(reportedException.getClass()),
              String.format(
                  "Record index %d. Expected %s, actual %s",
                  i, expectedException, reportedException));
        } else {
          if (!reported.isEmpty()) {
            InMemoryErrorReporter.ReportedData reportedData = reported.peek();
            assertNotEquals(
                record,
                reportedData.record(),
                String.format(
                    "Record index %d. Did not expect but encountered %s", i, reportedData));
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
