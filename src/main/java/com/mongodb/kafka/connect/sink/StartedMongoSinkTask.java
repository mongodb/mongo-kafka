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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.BULK_WRITE_ORDERED_CONFIG;
import static com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.dlq.AnalyzedBatchFailedWithBulkWriteException;
import com.mongodb.kafka.connect.sink.dlq.ErrorReporter;
import com.mongodb.kafka.connect.util.jmx.SinkTaskStatistics;
import com.mongodb.kafka.connect.util.jmx.Timer;
import com.mongodb.kafka.connect.util.jmx.internal.MBeanServerUtils;

public final class StartedMongoSinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);

  private final MongoSinkConfig sinkConfig;
  private final MongoClient mongoClient;
  private final ErrorReporter errorReporter;
  private final Set<MongoNamespace> checkedTimeseriesNamespaces;

  private final SinkTaskStatistics statistics;
  private Timer lastTaskInvocation = null;

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
  }

  private String getMBeanName() {
    String id = MBeanServerUtils.taskIdFromCurrentThread();
    return "com.mongodb.kafka.connect:type=sink-task-metrics,task=sink-task-" + id;
  }

  /** @see MongoSinkTask#stop() */
  void stop() {
    mongoClient.close();
    MBeanServerUtils.unregisterMBean(getMBeanName());
  }

  /** @see MongoSinkTask#put(Collection) */
  void put(final Collection<SinkRecord> records) {
    if (lastTaskInvocation != null) {
      statistics
          .getInConnectFramework()
          .sample(lastTaskInvocation.getElapsedTime(TimeUnit.MILLISECONDS));
    }
    Timer taskTime = Timer.start();
    statistics.getRecords().sample(records.size());
    trackLatestRecordTimestampOffset(records);
    if (records.isEmpty()) {
      LOGGER.debug("No sink records to process for current poll operation");
    } else {
      Timer processingTime = Timer.start();
      List<List<MongoProcessedSinkRecordData>> batches =
          MongoSinkRecordProcessor.orderedGroupByTopicAndNamespace(
              records, sinkConfig, errorReporter);
      statistics.getProcessingPhases().sample(processingTime.getElapsedTime(TimeUnit.MILLISECONDS));
      for (List<MongoProcessedSinkRecordData> batch : batches) {
        bulkWriteBatch(batch);
      }
    }
    statistics.getInTaskPut().sample(taskTime.getElapsedTime(TimeUnit.MILLISECONDS));
    lastTaskInvocation = Timer.start();
    if (LOGGER.isDebugEnabled()) {
      // toJSON relatively expensive
      LOGGER.debug(statistics.getName() + ": " + statistics.toJSON());
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
      statistics.getBatchWritesSuccessful().sample(writeTime.getElapsedTime(TimeUnit.MILLISECONDS));
      statistics.getRecordsSuccessful().sample(batch.size());
      LOGGER.debug("Mongodb bulk write result: {}", result);
    } catch (RuntimeException e) {
      statistics.getBatchWritesFailed().sample(writeTime.getElapsedTime(TimeUnit.MILLISECONDS));
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
      if (logErrors) {
        LOGGER.error(
            "Failed to put into the sink some records, see log entries below for the details", e);
        analyzedBatch.log();
      }
      if (tolerateErrors) {
        analyzedBatch.report();
      } else {
        throw new DataException(e);
      }
    } else {
      if (logErrors) {
        log(batch, e);
      }
      if (!tolerateErrors) {
        throw new DataException(e);
      }
    }
  }

  private static void log(final Collection<SinkRecord> records, final RuntimeException e) {
    LOGGER.error("Failed to put into the sink the following records: {}", records, e);
  }
}
