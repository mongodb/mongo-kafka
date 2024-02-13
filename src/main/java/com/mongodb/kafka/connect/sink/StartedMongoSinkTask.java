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

import static com.mongodb.kafka.connect.sink.MongoSinkTask.LOGGER;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.BULK_WRITE_ORDERED_CONFIG;
import static com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.dlq.AnalyzedBatchFailedWithBulkWriteException;
import com.mongodb.kafka.connect.sink.dlq.ErrorReporter;
import com.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;
import com.mongodb.kafka.connect.util.jmx.SinkTaskStatistics;
import com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils;
import com.mongodb.kafka.connect.util.time.InnerOuterTimer;
import com.mongodb.kafka.connect.util.time.InnerOuterTimer.InnerTimer;
import com.mongodb.kafka.connect.util.time.Timer;

final class StartedMongoSinkTask implements AutoCloseable {
  private final MongoSinkConfig sinkConfig;
  private final MongoClient mongoClient;
  private final ErrorReporter errorReporter;
  private final Set<MongoNamespace> checkedTimeseriesNamespaces;

  private final SinkTaskStatistics statistics;
  private final InnerOuterTimer inTaskPutInConnectFrameworkTimer;
  private static final int DUPLICATE_KYE_ERROR_CODE = 11000;
  // TODO move this flag to settings
  private final boolean ignoreDuplicateKeyError = true;

  StartedMongoSinkTask(
      final MongoSinkConfig sinkConfig,
      final MongoClient mongoClient,
      final ErrorReporter errorReporter) {
    this.sinkConfig = sinkConfig;
    this.mongoClient = mongoClient;
    this.errorReporter = errorReporter;
    checkedTimeseriesNamespaces = new HashSet<>();
    statistics = new SinkTaskStatistics(getMBeanName());
    statistics.register();
    inTaskPutInConnectFrameworkTimer =
        InnerOuterTimer.start(
            (inTaskPutSample) -> {
              statistics.getInTaskPut().sample(inTaskPutSample.toMillis());
              if (LOGGER.isDebugEnabled()) {
                // toJSON relatively expensive
                LOGGER.debug(statistics.getName() + ": " + statistics.toJSON());
              }
            },
            (inFrameworkSample) ->
                statistics.getInConnectFramework().sample(inFrameworkSample.toMillis()));
  }

  private String getMBeanName() {
    String id = MBeanServerUtils.taskIdFromCurrentThread();
    String connectorName = JmxStatisticsManager.getConnectorName(this.sinkConfig.getOriginals());
    return "com.mongodb.kafka.connect:type=sink-task-metrics,connector="
        + connectorName
        + ",task=sink-task-"
        + id;
  }

  /** @see MongoSinkTask#stop() */
  @SuppressWarnings("try")
  @Override
  public void close() {
    try (MongoClient autoCloseable = mongoClient) {
      statistics.unregister();
    }
  }

  /** @see MongoSinkTask#put(Collection) */
  @SuppressWarnings("try")
  void put(final Collection<SinkRecord> records) {
    try (InnerTimer automatic = inTaskPutInConnectFrameworkTimer.sampleOuter()) {
      statistics.getRecords().sample(records.size());
      trackLatestRecordTimestampOffset(records);
      if (records.isEmpty()) {
        LOGGER.debug("No sink records to process for current poll operation");
      } else {
        Timer processingTime = Timer.start();
        List<List<MongoProcessedSinkRecordData>> batches =
            MongoSinkRecordProcessor.orderedGroupByTopicAndNamespace(
                records, sinkConfig, errorReporter);
        statistics.getProcessingPhases().sample(processingTime.getElapsedTime().toMillis());
        for (List<MongoProcessedSinkRecordData> batch : batches) {
          bulkWriteBatch(batch);
        }
      }
    }
  }

  private void trackLatestRecordTimestampOffset(final Collection<SinkRecord> records) {
    OptionalLong latestRecord =
        records.stream()
            .filter(v -> v.timestamp() != null)
            .mapToLong(ConnectRecord::timestamp)
            .max();
    if (latestRecord.isPresent()) {
      long offsetMs = System.currentTimeMillis() - latestRecord.getAsLong();
      statistics.getLatestKafkaTimeDifferenceMs().sample(offsetMs);
    }
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
    boolean bulkWriteOrdered = config.getBoolean(BULK_WRITE_ORDERED_CONFIG);

    Timer writeTime = Timer.start();
    try {
      LOGGER.debug(
          "Bulk writing {} document(s) into collection [{}] via an {} bulk write",
          writeModels.size(),
          namespace.getFullName(),
          bulkWriteOrdered ? "ordered" : "unordered");
      BulkWriteResult result =
          mongoClient
              .getDatabase(namespace.getDatabaseName())
              .getCollection(namespace.getCollectionName(), BsonDocument.class)
              .bulkWrite(writeModels, new BulkWriteOptions().ordered(bulkWriteOrdered));
      statistics.getBatchWritesSuccessful().sample(writeTime.getElapsedTime().toMillis());
      statistics.getRecordsSuccessful().sample(batch.size());
      LOGGER.debug("Mongodb bulk write result: {}", result);
    } catch (RuntimeException e) {
      statistics.getBatchWritesFailed().sample(writeTime.getElapsedTime().toMillis());
      statistics.getRecordsFailed().sample(batch.size());
      handleTolerableWriteException(
          batch.stream()
              .map(MongoProcessedSinkRecordData::getSinkRecord)
              .collect(Collectors.toList()),
          bulkWriteOrdered,
          e,
          config.logErrors(),
          config.tolerateErrors());
    }
    checkRateLimit(config);
  }

  private void checkTimeseries(final MongoNamespace namespace, final MongoSinkTopicConfig config) {
    if (!checkedTimeseriesNamespaces.contains(namespace)) {
      if (config.isTimeseries()) {
        validateCollection(mongoClient, namespace, config);
      }
      checkedTimeseriesNamespaces.add(namespace);
    }
  }

  private static void checkRateLimit(final MongoSinkTopicConfig config) {
    RateLimitSettings rls = config.getRateLimitSettings();
    if (rls.isTriggered()) {
      LOGGER.debug(
          "Rate limit settings triggering {}ms defer timeout after processing {} further batches for topic {}",
          rls.getTimeoutMs(),
          rls.getEveryN(),
          config.getTopic());
      try {
        Thread.sleep(rls.getTimeoutMs());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new DataException("Rate limiting was interrupted", e);
      }
    }
  }

  private void handleTolerableWriteException(
      final List<SinkRecord> batch,
      final boolean ordered,
      final RuntimeException e,
      final boolean logErrors,
      final boolean tolerateErrors) {
    if (e instanceof MongoBulkWriteException) {
      AnalyzedBatchFailedWithBulkWriteException analyzedBatch =
          new AnalyzedBatchFailedWithBulkWriteException(
              batch,
              ordered,
              (MongoBulkWriteException) e,
              errorReporter,
              StartedMongoSinkTask::log);
      List<BulkWriteError> errors = ((MongoBulkWriteException) e).getWriteErrors();
      if (logErrors) {
        LOGGER.error(
            "Failed to put into the sink some records, see log entries below for the details", e);
        analyzedBatch.log();
      }
      if (tolerateErrors) {
        analyzedBatch.report();
      } else if (errors.size() == 1
          && errors.get(0).getCode() == DUPLICATE_KYE_ERROR_CODE
          && ignoreDuplicateKeyError) {
        LOGGER.error("Failed to put into the sink some records: Duplicate Key Error");
      } else {
        throw new DataException(e);
      }
    } else {
      if (logErrors) {
        log(batch, e);
      }
      if (tolerateErrors) {
        batch.forEach(record -> errorReporter.report(record, e));
      } else {
        throw new DataException(e);
      }
    }
  }

  private static void log(final Collection<SinkRecord> records, final RuntimeException e) {
    LOGGER.error("Failed to put into the sink the following records: {}", records, e);
  }
}
