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

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.PROVIDER_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;
import static com.mongodb.kafka.connect.util.ServerApiConfig.setServerApi;
import static com.mongodb.kafka.connect.util.VisibleForTesting.AccessModifier.PRIVATE;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import com.mongodb.kafka.connect.Versions;
import com.mongodb.kafka.connect.sink.dlq.ErrorReporter;
import com.mongodb.kafka.connect.util.VisibleForTesting;

public class MongoSinkTask extends SinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);
  private static final String CONNECTOR_TYPE = "sink";
  private StartedMongoSinkTask startedTask;

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
    MongoSinkConfig sinkConfig;
    try {
      sinkConfig = new MongoSinkConfig(props);
    } catch (Exception e) {
      throw new ConnectException("Failed to start new task", e);
    }
    startedTask =
        new StartedMongoSinkTask(sinkConfig, createMongoClient(sinkConfig), createErrorReporter());
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
    startedTask.put(records);
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
    if (startedTask != null) {
      startedTask.stop();
    }
  }

  private ErrorReporter createErrorReporter() {
    ErrorReporter result = nopErrorReporter();
    if (context != null) {
      try {
        ErrantRecordReporter errantRecordReporter = context.errantRecordReporter();
        if (errantRecordReporter != null) {
          result = errantRecordReporter::report;
        } else {
          LOGGER.info("Errant record reporter not configured.");
        }
      } catch (NoClassDefFoundError | NoSuchMethodError e) {
        // Will occur in Connect runtimes earlier than 2.6
        LOGGER.info("Kafka versions prior to 2.6 do not support the errant record reporter.");
      }
    }
    return result;
  }

  @VisibleForTesting(otherwise = PRIVATE)
  static ErrorReporter nopErrorReporter() {
    return (record, e) -> {};
  }

  private static MongoClient createMongoClient(final MongoSinkConfig sinkConfig) {
    MongoClientSettings.Builder builder =
        MongoClientSettings.builder().applyConnectionString(sinkConfig.getConnectionString());
    setServerApi(builder, sinkConfig);
    return MongoClients.create(
        builder.build(),
        getMongoDriverInformation(CONNECTOR_TYPE, sinkConfig.getString(PROVIDER_CONFIG)));
  }
}
