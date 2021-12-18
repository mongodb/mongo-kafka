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

import static com.mongodb.kafka.connect.util.Assertions.assertNotNull;
import static com.mongodb.kafka.connect.util.Assertions.assertTrue;
import static com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection;
import static java.util.Collections.singletonList;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.lang.Nullable;

final class StartedMongoSinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);
  private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();

  private final MongoSinkConfig sinkConfig;
  private final MongoClient mongoClient;
  private final BiConsumer<SinkRecord, Exception> errorReporter;
  private final Set<MongoNamespace> checkedTimeseriesNamespaces;

  StartedMongoSinkTask(
      final MongoSinkConfig sinkConfig,
      final MongoClient mongoClient,
      final BiConsumer<SinkRecord, Exception> errorReporter) {
    this.sinkConfig = sinkConfig;
    this.mongoClient = mongoClient;
    this.errorReporter = errorReporter;
    checkedTimeseriesNamespaces = new HashSet<>();
  }

  /** @see MongoSinkTask#stop() */
  void stop() {
    mongoClient.close();
  }

  /** @see MongoSinkTask#put(Collection) */
  void put(final Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      LOGGER.debug("No sink records to process for current poll operation");
      return;
    }
    MongoSinkRecordProcessor.orderedGroupByTopicAndNamespace(records, sinkConfig, errorReporter)
        .forEach(this::bulkWriteBatch);
  }

  private void bulkWriteBatch(final List<MongoProcessedSinkRecordData> batch) {
    if (batch.isEmpty()) {
      return;
    }

    MongoNamespace namespace = batch.get(0).getNamespace();
    MongoSinkTopicConfig config = batch.get(0).getConfig();
    checkTimeseries(namespace, config);

    List<WriteModel<BsonDocument>> writeModels =
        batch.stream()
            .map(MongoProcessedSinkRecordData::getWriteModel)
            .collect(Collectors.toList());

    try {
      LOGGER.debug(
          "Bulk writing {} document(s) into collection [{}]",
          writeModels.size(),
          namespace.getFullName());
      BulkWriteResult result =
          mongoClient
              .getDatabase(namespace.getDatabaseName())
              .getCollection(namespace.getCollectionName(), BsonDocument.class)
              .bulkWrite(writeModels, BULK_WRITE_OPTIONS);
      LOGGER.debug("Mongodb bulk write result: {}", result);
      checkRateLimit(config);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DataException("Rate limiting was interrupted", e);
    } catch (RuntimeException e) {
      handleTolerableWriteException(batch, e, config.logErrors(), config.tolerateErrors());
    }
  }

  private void checkTimeseries(final MongoNamespace namespace, final MongoSinkTopicConfig config) {
    if (!checkedTimeseriesNamespaces.contains(namespace)) {
      if (config.isTimeseries()) {
        validateCollection(mongoClient, namespace, config);
      }
      checkedTimeseriesNamespaces.add(namespace);
    }
  }

  private static void checkRateLimit(final MongoSinkTopicConfig config)
      throws InterruptedException {
    RateLimitSettings rls = config.getRateLimitSettings();

    if (rls.isTriggered()) {
      LOGGER.debug(
          "Rate limit settings triggering {}ms defer timeout after processing {}"
              + " further batches for topic {}",
          rls.getTimeoutMs(),
          rls.getEveryN(),
          config.getTopic());
      Thread.sleep(rls.getTimeoutMs());
    }
  }

  private void handleTolerableWriteException(
      final List<MongoProcessedSinkRecordData> batch,
      final RuntimeException e,
      final boolean logErrors,
      final boolean tolerateErrors) {
    List<SinkRecord> records =
        batch.stream()
            .map(MongoProcessedSinkRecordData::getSinkRecord)
            .collect(Collectors.toList());
    if (e instanceof MongoBulkWriteException) {
      AnalyzedBatchFailedWithBulkWriteException analyzedBatch =
          new AnalyzedBatchFailedWithBulkWriteException(records, (MongoBulkWriteException) e);
      if (logErrors) {
        analyzedBatch.log();
      }
      if (tolerateErrors) {
        analyzedBatch.report();
      } else {
        throw new DataException(e);
      }
    } else {
      if (logErrors) {
        log(records, e);
      }
      if (!tolerateErrors) {
        throw new DataException(e);
      }
    }
  }

  private class AnalyzedBatchFailedWithBulkWriteException {
    private final List<SinkRecord> batch;
    private final MongoBulkWriteException e;
    private final Map<Integer, Entry<SinkRecord, WriteException>> recordsFailedWithWriteError =
        new HashMap<>();
    private final Map<Integer, SinkRecord> recordsFailedWithWriteConcernError = new HashMap<>();
    private final Map<Integer, SinkRecord> skippedRecords = new HashMap<>();
    @Nullable private final WriteConcernException writeConcernException;
    private final WriteSkippedException writeSkippedException = new WriteSkippedException();

    AnalyzedBatchFailedWithBulkWriteException(
        final List<SinkRecord> batch, final MongoBulkWriteException e) {
      this.batch = batch;
      this.e = e;
      WriteConcernError writeConcernError = e.getWriteConcernError();
      writeConcernException =
          writeConcernError == null ? null : new WriteConcernException(writeConcernError);
      analyze();
    }

    private void analyze() {
      List<BulkWriteError> writeErrors = e.getWriteErrors();
      WriteConcernError writeConcernError = e.getWriteConcernError();
      if (writeErrors.isEmpty()) {
        assertNotNull(writeConcernError);
        for (int i = 0; i < batch.size(); i++) {
          recordsFailedWithWriteConcernError.put(i, batch.get(i));
        }
      } else {
        assertTrue(writeErrors.size() == 1);
        BulkWriteError writeError = writeErrors.get(0);
        int writeErrorIdx = writeError.getIndex();
        recordsFailedWithWriteError.put(
            writeErrorIdx,
            new SimpleImmutableEntry<>(batch.get(writeErrorIdx), new WriteException(writeError)));
        for (int i = writeErrorIdx + 1; i < batch.size(); i++) {
          skippedRecords.put(i, batch.get(i));
        }
        if (writeConcernError != null) {
          for (int i = 0; i < writeErrorIdx; i++) {
            recordsFailedWithWriteConcernError.put(i, batch.get(i));
          }
        }
      }
    }

    void log() {
      if (!recordsFailedWithWriteError.isEmpty()) {
        recordsFailedWithWriteError.forEach(
            (i, recordFailedWithWriteError) ->
                StartedMongoSinkTask.log(
                    singletonList(recordFailedWithWriteError.getKey()),
                    recordFailedWithWriteError.getValue()));
      }
      if (!recordsFailedWithWriteConcernError.isEmpty()) {
        StartedMongoSinkTask.log(
            recordsFailedWithWriteConcernError.values(), assertNotNull(writeConcernException));
      }
      if (!skippedRecords.isEmpty()) {
        StartedMongoSinkTask.log(skippedRecords.values(), writeSkippedException);
      }
      LOGGER.error(null, e);
    }

    /**
     * {@linkplain #errorReporter Reports} records in the batch that either definitely have not been
     * written, or may not have been written. While it is unclear whether the order of reported records in the DLQ is the same as the order
     * in which they are reported, we still report them in the order they are present in the batch just in case because it is trivial to do.
     */
    void report() {
      for (int i = 0; i < batch.size(); i++) {
        Entry<SinkRecord, WriteException> recordFailedWithWriteError =
            recordsFailedWithWriteError.get(i);
        if (recordFailedWithWriteError != null) {
          errorReporter.accept(
              recordFailedWithWriteError.getKey(), recordFailedWithWriteError.getValue());
          continue;
        }
        SinkRecord recordFailedWithWriteConcernError = recordsFailedWithWriteConcernError.get(i);
        if (recordFailedWithWriteConcernError != null) {
          errorReporter.accept(
              recordFailedWithWriteConcernError, assertNotNull(writeConcernException));
          continue;
        }
        SinkRecord skippedRecord = skippedRecords.get(i);
        if (skippedRecord != null) {
          errorReporter.accept(skippedRecord, writeSkippedException);
        }
      }
    }
  }

  private static void log(final Collection<SinkRecord> records, final RuntimeException e) {
    LOGGER.error("Failed to put into the sink the following records: {}", records, e);
  }
}
