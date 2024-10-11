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

import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PROVIDER_CONFIG;
import static com.mongodb.kafka.connect.util.Assertions.assertNotNull;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;
import static com.mongodb.kafka.connect.util.ServerApiConfig.setServerApi;
import static com.mongodb.kafka.connect.util.SslConfigs.setupSsl;
import static java.util.Collections.singletonMap;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;

import com.mongodb.kafka.connect.Versions;
import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
import com.mongodb.kafka.connect.source.statistics.JmxStatisticsManager;
import com.mongodb.kafka.connect.source.statistics.StatisticsManager;
import com.mongodb.kafka.connect.util.ResumeTokenUtils;
import com.mongodb.kafka.connect.util.VisibleForTesting;
import com.mongodb.kafka.connect.util.jmx.SourceTaskStatistics;

/**
 * A Kafka Connect source task that uses change streams to broadcast changes to the collection,
 * database or client.
 *
 * <h2>Copy Existing Data</h2>
 *
 * <p>If configured the connector will copy the existing data from the collection, database or
 * client. All namespaces that exist at the time of starting the task will be broadcast onto the
 * topic as insert operations. Only when all the data from all namespaces have been broadcast will
 * the change stream cursor start broadcasting new changes. The logic for copying existing data is
 * as follows:
 *
 * <ol>
 *   <li>Get the latest resumeToken from MongoDB
 *   <li>Create insert events for all configured namespaces using multiple threads. This step is
 *       completed only after <em>all</em> collections are successfully copied.
 *   <li>Start a change stream cursor from the saved resumeToken
 * </ol>
 *
 * <p>It should be noted that the reading of all the data during the copy and then the subsequent
 * change stream events may produce duplicated events. During the copy, clients can make changes to
 * the data in MongoDB, which may be represented both by the copying process and the change stream.
 * However, as the change stream events are idempotent the changes can be applied so that the data
 * is eventually consistent.
 *
 * <p>It should also be noted renaming a collection during the copying process is not supported.
 *
 * <h3>Restarts</h3>
 *
 * Restarting the connector during the copying phase, will cause the whole copy process to restart.
 * Restarts after the copying process will resume from the last seen resumeToken.
 */
public final class MongoSourceTask extends SourceTask {
  static final Logger LOGGER = LoggerFactory.getLogger(MongoSourceTask.class);
  private static final String CONNECTOR_TYPE = "source";
  public static final String ID_FIELD = "_id";
  public static final String DOCUMENT_KEY_FIELD = "documentKey";
  static final String COPY_KEY = "copy";
  private static final String NS_KEY = "ns";
  private static final int UNKNOWN_FIELD_ERROR = 40415;
  private static final int FAILED_TO_PARSE_ERROR = 9;

  private StartedMongoSourceTask startedTask;

  @Override
  public String version() {
    return Versions.VERSION;
  }

  @SuppressWarnings("try")
  @Override
  public void start(final Map<String, String> props) {
    LOGGER.info("Starting MongoDB source task");
    StatisticsManager statisticsManager = null;
    MongoClient mongoClient = null;
    MongoCopyDataManager copyDataManager = null;
    try {
      MongoSourceConfig sourceConfig = new MongoSourceConfig(props);
      boolean shouldCopyData = shouldCopyData(context, sourceConfig);
      String connectorName = JmxStatisticsManager.getConnectorName(props);
      statisticsManager = new JmxStatisticsManager(shouldCopyData, connectorName);
      StatisticsManager statsManager = statisticsManager;
      CommandListener statisticsCommandListener =
          new CommandListener() {
            @Override
            public void commandSucceeded(final CommandSucceededEvent event) {
              mongoCommandSucceeded(event, statsManager.currentStatistics());
            }

            @Override
            public void commandFailed(final CommandFailedEvent event) {
              mongoCommandFailed(event, statsManager.currentStatistics());
            }
          };

      MongoClientSettings.Builder builder =
          MongoClientSettings.builder()
              .applyConnectionString(sourceConfig.getConnectionString())
              .addCommandListener(statisticsCommandListener)
              .applyToSslSettings(sslBuilder -> setupSsl(sslBuilder, sourceConfig));

      if (sourceConfig.getCustomCredentialProvider() != null) {
        builder.credential(
            sourceConfig
                .getCustomCredentialProvider()
                .getCustomCredential(sourceConfig.originals()));
      }

      setServerApi(builder, sourceConfig);

      mongoClient =
          MongoClients.create(
              builder.build(),
              getMongoDriverInformation(CONNECTOR_TYPE, sourceConfig.getString(PROVIDER_CONFIG)));
      copyDataManager = shouldCopyData ? new MongoCopyDataManager(sourceConfig, mongoClient) : null;

      startedTask =
          new StartedMongoSourceTask(
              // It is safer to read the `context` reference each time we need it
              // in case it changes, because there is no
              // documentation stating that it cannot be changed.
              () -> context, sourceConfig, mongoClient, copyDataManager, statisticsManager);
    } catch (RuntimeException taskStartingException) {
      //noinspection EmptyTryBlock
      try (StatisticsManager autoCloseableStatisticsManager = statisticsManager;
          MongoClient autoCloseableMongoClient = mongoClient;
          MongoCopyDataManager autoCloseableCopyDataManager = copyDataManager) {
        // just using try-with-resources to ensure they all get closed, even in the case of
        // exceptions
      } catch (RuntimeException resourceReleasingException) {
        taskStartingException.addSuppressed(resourceReleasingException);
      }
      throw new ConnectException("Failed to start MongoDB source task", taskStartingException);
    }
    LOGGER.info("Started MongoDB source task");
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  StartedMongoSourceTask startedTask() {
    return assertNotNull(startedTask);
  }

  @Override
  public List<SourceRecord> poll() {
    return startedTask.poll();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping MongoDB source task");
    if (startedTask != null) {
      startedTask.close();
    }
  }

  static boolean doesNotSupportsStartAfter(final MongoCommandException e) {
    return ((e.getErrorCode() == FAILED_TO_PARSE_ERROR || e.getErrorCode() == UNKNOWN_FIELD_ERROR)
        && e.getErrorMessage().contains("startAfter"));
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static Map<String, Object> createPartitionMap(final MongoSourceConfig sourceConfig) {
    String partitionName = sourceConfig.getString(MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG);
    if (partitionName.isEmpty()) {
      partitionName = createDefaultPartitionName(sourceConfig);
    }
    return singletonMap(NS_KEY, partitionName);
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static String createDefaultPartitionName(final MongoSourceConfig sourceConfig) {
    ConnectionString connectionString = sourceConfig.getConnectionString();
    StringBuilder builder = new StringBuilder();
    builder.append(connectionString.isSrvProtocol() ? "mongodb+srv://" : "mongodb://");
    builder.append(String.join(",", connectionString.getHosts()));
    builder.append("/");
    builder.append(sourceConfig.getString(DATABASE_CONFIG));
    if (!sourceConfig.getString(COLLECTION_CONFIG).isEmpty()) {
      builder.append(".");
      builder.append(sourceConfig.getString(COLLECTION_CONFIG));
    }
    return builder.toString();
  }

  /**
   * Checks to see if data should be copied.
   *
   * <p>Copying data is only required if it's been configured and it hasn't already completed.
   *
   * @return true if should copy the existing data.
   */
  private static boolean shouldCopyData(
      final SourceTaskContext context, final MongoSourceConfig sourceConfig) {
    Map<String, Object> offset = getOffset(context, sourceConfig);
    return sourceConfig.getStartupConfig().startupMode() == StartupMode.COPY_EXISTING
        && (offset == null || (offset.containsKey(COPY_KEY) && Boolean.parseBoolean(offset.get(COPY_KEY).toString())));
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static Map<String, Object> getOffset(
      final SourceTaskContext context, final MongoSourceConfig sourceConfig) {
    if (context != null) {
      return context.offsetStorageReader().offset(createPartitionMap(sourceConfig));
    }
    return null;
  }

  @Override
  public void commitRecord(final SourceRecord record, final RecordMetadata metadata) {
    startedTask.commitRecord(record, metadata);
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static void mongoCommandSucceeded(
      final CommandSucceededEvent event, final SourceTaskStatistics currentStatistics) {
    String commandName = event.getCommandName();
    long elapsedTimeMs = event.getElapsedTime(TimeUnit.MILLISECONDS);
    if ("getMore".equals(commandName)) {
      currentStatistics.getGetmoreCommandsSuccessful().sample(elapsedTimeMs);
    } else if ("aggregate".equals(commandName) || "find".equals(commandName)) {
      currentStatistics.getInitialCommandsSuccessful().sample(elapsedTimeMs);
    }
    ResumeTokenUtils.getResponseOffsetSecs(event.getResponse())
        .ifPresent(offset -> currentStatistics.getLatestMongodbTimeDifferenceSecs().sample(offset));
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  static void mongoCommandFailed(
      final CommandFailedEvent event, final SourceTaskStatistics currentStatistics) {
    Throwable e = event.getThrowable();
    if (e instanceof MongoCommandException) {
      if (doesNotSupportsStartAfter((MongoCommandException) e)) {
        // silently ignore this expected exception, which is used to set this.supportsStartAfter
        return;
      }
    }
    String commandName = event.getCommandName();
    long elapsedTimeMs = event.getElapsedTime(TimeUnit.MILLISECONDS);
    if ("getMore".equals(commandName)) {
      currentStatistics.getGetmoreCommandsFailed().sample(elapsedTimeMs);
    } else if ("aggregate".equals(commandName) || "find".equals(commandName)) {
      currentStatistics.getInitialCommandsFailed().sample(elapsedTimeMs);
    }
  }
}
