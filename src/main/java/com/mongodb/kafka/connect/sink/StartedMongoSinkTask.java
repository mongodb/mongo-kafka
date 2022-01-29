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
import java.util.Set;
import java.util.stream.Collectors;

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

public final class StartedMongoSinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);

  private final MongoSinkConfig sinkConfig;
  private final MongoClient mongoClient;
  private final ErrorReporter errorReporter;
  private final Set<MongoNamespace> checkedTimeseriesNamespaces;

  StartedMongoSinkTask(
      final MongoSinkConfig sinkConfig,
      final MongoClient mongoClient,
      final ErrorReporter errorReporter) {
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
    boolean bulkWriteOrdered = config.getBoolean(BULK_WRITE_ORDERED_CONFIG);

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
      LOGGER.debug("Mongodb bulk write result: {}", result);
      checkRateLimit(config);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DataException("Rate limiting was interrupted", e);
    } catch (RuntimeException e) {
      handleTolerableWriteException(
          batch.stream()
              .map(MongoProcessedSinkRecordData::getSinkRecord)
              .collect(Collectors.toList()),
          bulkWriteOrdered,
          e,
          config.logErrors(),
          config.tolerateErrors());
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
