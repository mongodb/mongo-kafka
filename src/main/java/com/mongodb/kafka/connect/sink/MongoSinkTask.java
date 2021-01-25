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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_NUM_RETRIES_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.RETRIES_DEFER_TIMEOUT_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;
import static java.util.Collections.emptyList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.Versions;

public class MongoSinkTask extends SinkTask {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);
  private static final String CONNECTOR_TYPE = "sink";
  private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();
  private MongoSinkConfig sinkConfig;
  private MongoClient mongoClient;
  private Map<String, AtomicInteger> remainingRetriesTopicMap;

  @Override
  public String version() {
    return Versions.VERSION;
  }

  /**
   * Start the Task. This should handle any configuration parsing and one-time setup of the task.
   *
   * @param props initial configuration
   */
  @Override
  public void start(final Map<String, String> props) {
    LOGGER.info("Starting MongoDB sink task");
    try {
      sinkConfig = new MongoSinkConfig(props);
      remainingRetriesTopicMap =
          new ConcurrentHashMap<>(
              sinkConfig.getTopics().orElse(emptyList()).stream()
                  .collect(
                      Collectors.toMap(
                          (t) -> t,
                          (t) ->
                              new AtomicInteger(
                                  sinkConfig
                                      .getMongoSinkTopicConfig(t)
                                      .getInt(MAX_NUM_RETRIES_CONFIG)))));
    } catch (Exception e) {
      throw new ConnectException("Failed to start new task", e);
    }
    LOGGER.debug("Started MongoDB sink task");
  }

  /**
   * Put the records in the sink. Usually this should send the records to the sink asynchronously
   * and immediately return.
   *
   * <p>If this operation fails, the SinkTask may throw a {@link
   * org.apache.kafka.connect.errors.RetriableException} to indicate that the framework should
   * attempt to retry the same call again. Other exceptions will cause the task to be stopped
   * immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before
   * the batch will be retried.
   *
   * @param records the set of records to send
   */
  @Override
  public void put(final Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      LOGGER.debug("No sink records to process for current poll operation");
      return;
    }
    MongoSinkRecordProcessor.groupByTopicAndNamespace(records, sinkConfig)
        .forEach(this::bulkWriteBatch);
  }

  /**
   * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
   *
   * @param currentOffsets the current offset state as of the last call to {@link
   *     #put(Collection)}}, provided for convenience but could also be determined by tracking all
   *     offsets included in the {@link SinkRecord}s passed to {@link #put}.
   */
  @Override
  public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
    // NOTE: flush is not used for now...
    LOGGER.debug("Flush called - noop");
  }

  /**
   * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once
   * outstanding calls to other methods have completed (e.g., {@link #put(Collection)} has returned)
   * and a final {@link #flush(Map)} and offset commit has completed. Implementations of this method
   * should only need to perform final cleanup operations, such as closing network connections to
   * the sink system.
   */
  @Override
  public void stop() {
    LOGGER.info("Stopping MongoDB sink task");
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  private MongoClient getMongoClient() {
    if (mongoClient == null) {
      mongoClient =
          MongoClients.create(
              sinkConfig.getConnectionString(), getMongoDriverInformation(CONNECTOR_TYPE));
    }
    return mongoClient;
  }

  private void bulkWriteBatch(final List<MongoProcessedSinkRecordData> batch) {
    List<WriteModel<BsonDocument>> writeModels =
        batch.stream()
            .filter(MongoProcessedSinkRecordData::canProcess)
            .map(MongoProcessedSinkRecordData::getWriteModel)
            .collect(Collectors.toList());

    if (writeModels.isEmpty()) {
      return;
    }

    MongoNamespace namespace = batch.get(0).getNamespace();
    MongoSinkTopicConfig config = batch.get(0).getConfig();
    try {
      LOGGER.debug(
          "Bulk writing {} document(s) into collection [{}]",
          writeModels.size(),
          namespace.getFullName());
      BulkWriteResult result =
          getMongoClient()
              .getDatabase(namespace.getDatabaseName())
              .getCollection(namespace.getCollectionName(), BsonDocument.class)
              .bulkWrite(writeModels, BULK_WRITE_OPTIONS);
      LOGGER.debug("Mongodb bulk write result: {}", result);
      resetRemainingRetriesForTopic(config);
      checkRateLimit(config);
    } catch (MongoException e) {
      LOGGER.warn(
          "Writing {} document(s) into collection [{}] failed.",
          writeModels.size(),
          namespace.getFullName());
      handleMongoException(config, e);
    } catch (Exception e) {
      if (!config.tolerateErrors()) {
        throw new DataException("Failed to write mongodb documents", e);
      }
    }
  }

  private void resetRemainingRetriesForTopic(final MongoSinkTopicConfig topicConfig) {
    getRemainingRetriesForTopic(topicConfig.getTopic())
        .set(topicConfig.getInt(MAX_NUM_RETRIES_CONFIG));
  }

  private void checkRateLimit(final MongoSinkTopicConfig config) {
    RateLimitSettings rls = config.getRateLimitSettings();

    if (rls.isTriggered()) {
      LOGGER.debug(
          "Rate limit settings triggering {}ms defer timeout after processing {}"
              + " further batches for topic {}",
          rls.getTimeoutMs(),
          rls.getEveryN(),
          config.getTopic());
      try {
        Thread.sleep(rls.getTimeoutMs());
      } catch (InterruptedException e) {
        LOGGER.warn(e.getMessage());
      }
    }
  }

  private AtomicInteger getRemainingRetriesForTopic(final String topic) {
    if (!remainingRetriesTopicMap.containsKey(topic)) {
      remainingRetriesTopicMap.put(
          topic,
          new AtomicInteger(
              sinkConfig.getMongoSinkTopicConfig(topic).getInt(MAX_NUM_RETRIES_CONFIG)));
    }
    return remainingRetriesTopicMap.get(topic);
  }

  private void handleMongoException(final MongoSinkTopicConfig config, final MongoException e) {
    if (getRemainingRetriesForTopic(config.getTopic()).decrementAndGet() <= 0) {
      if (config.logErrors()) {
        LOGGER.error("Error on mongodb operation", e);
        if (e instanceof MongoBulkWriteException) {
          LOGGER.error("Mongodb bulk write (partially) failed", e);
          LOGGER.error("WriteResult: {}", ((MongoBulkWriteException) e).getWriteResult());
          LOGGER.error("WriteErrors: {}", ((MongoBulkWriteException) e).getWriteErrors());
          LOGGER.error(
              "WriteConcernError: {}", ((MongoBulkWriteException) e).getWriteConcernError());
        }
      }
      if (!config.tolerateErrors()) {
        throw new DataException("Failed to write mongodb documents despite retrying", e);
      }
    } else {
      Integer deferRetryMs = config.getInt(RETRIES_DEFER_TIMEOUT_CONFIG);
      LOGGER.info("Deferring retry operation for {}ms", deferRetryMs);
      context.timeout(deferRetryMs);
      throw new RetriableException(e.getMessage(), e);
    }
  }
}
