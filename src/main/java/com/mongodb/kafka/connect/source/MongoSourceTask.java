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

import static com.mongodb.kafka.connect.source.MongoSourceConfig.BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG;
import static com.mongodb.kafka.connect.source.heartbeat.HeartbeatManager.HEARTBEAT_KEY;
import static com.mongodb.kafka.connect.source.producer.SchemaAndValueProducers.createKeySchemaAndValueProvider;
import static com.mongodb.kafka.connect.source.producer.SchemaAndValueProducers.createValueSchemaAndValueProvider;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import com.mongodb.kafka.connect.Versions;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;
import com.mongodb.kafka.connect.source.heartbeat.HeartbeatManager;
import com.mongodb.kafka.connect.source.producer.SchemaAndValueProducer;
import com.mongodb.kafka.connect.source.topic.mapping.TopicMapper;

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
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSourceTask.class);
  private static final String CONNECTOR_TYPE = "source";
  public static final String ID_FIELD = "_id";
  private static final String COPY_KEY = "copy";
  private static final String NS_KEY = "ns";
  private static final String FULL_DOCUMENT = "fullDocument";
  private static final int NAMESPACE_NOT_FOUND_ERROR = 26;
  private static final int INVALIDATED_RESUME_TOKEN_ERROR = 260;
  private static final int UNKNOWN_FIELD_ERROR = 40415;
  private static final int FAILED_TO_PARSE_ERROR = 9;
  private static final String RESUME_TOKEN = "resume token";
  private static final String NOT_FOUND = "not found";
  private static final String DOES_NOT_EXIST = "does not exist";
  private static final String INVALID_RESUME_TOKEN = "invalid resume token";

  private final Time time;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final AtomicBoolean isCopying = new AtomicBoolean();

  private MongoSourceConfig sourceConfig;
  private Map<String, Object> partitionMap;
  private MongoClient mongoClient;
  private HeartbeatManager heartbeatManager;

  private boolean supportsStartAfter = true;
  private boolean invalidatedCursor = false;
  private MongoCopyDataManager copyDataManager;
  private BsonDocument cachedResult;
  private BsonDocument cachedResumeToken;

  private MongoChangeStreamCursor<? extends BsonDocument> cursor;

  public MongoSourceTask() {
    this(new SystemTime());
  }

  private MongoSourceTask(final Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Versions.VERSION;
  }

  @Override
  public void start(final Map<String, String> props) {
    LOGGER.info("Starting MongoDB source task");
    try {
      sourceConfig = new MongoSourceConfig(props);
    } catch (Exception e) {
      throw new ConnectException("Failed to start new task", e);
    }

    heartbeatManager = null;
    partitionMap = null;
    createPartitionMap(sourceConfig);

    mongoClient =
        MongoClients.create(
            sourceConfig.getConnectionString(), getMongoDriverInformation(CONNECTOR_TYPE));
    if (shouldCopyData()) {
      setCachedResultAndResumeToken();
      copyDataManager = new MongoCopyDataManager(sourceConfig, mongoClient);
      isCopying.set(true);
    } else {
      initializeCursorAndHeartbeatManager(time, sourceConfig, mongoClient);
    }
    isRunning.set(true);
    LOGGER.info("Started MongoDB source task");
  }

  @Override
  public List<SourceRecord> poll() {
    final long startPoll = time.milliseconds();
    LOGGER.debug("Polling Start: {}", startPoll);
    List<SourceRecord> sourceRecords = new ArrayList<>();
    TopicMapper topicMapper = sourceConfig.getTopicMapper();
    boolean publishFullDocumentOnly = sourceConfig.getBoolean(PUBLISH_FULL_DOCUMENT_ONLY_CONFIG);
    int maxBatchSize = sourceConfig.getInt(POLL_MAX_BATCH_SIZE_CONFIG);
    long nextUpdate = startPoll + sourceConfig.getLong(POLL_AWAIT_TIME_MS_CONFIG);
    Map<String, Object> partition = createPartitionMap(sourceConfig);

    SchemaAndValueProducer keySchemaAndValueProducer =
        createKeySchemaAndValueProvider(sourceConfig);
    SchemaAndValueProducer valueSchemaAndValueProducer =
        createValueSchemaAndValueProvider(sourceConfig);

    while (isRunning.get()) {
      Optional<BsonDocument> next = getNextDocument();
      long untilNext = nextUpdate - time.milliseconds();

      if (!next.isPresent()) {
        if (untilNext > 0) {
          LOGGER.debug("Waiting {} ms to poll", untilNext);
          time.sleep(untilNext);
          continue; // Re-check stop flag before continuing
        }
        if (!sourceRecords.isEmpty()) {
          return sourceRecords;
        }
        if (heartbeatManager != null) {
          return heartbeatManager.heartbeat().map(Collections::singletonList).orElse(null);
        }
        return null;
      } else {
        BsonDocument changeStreamDocument = next.get();

        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, changeStreamDocument.getDocument(ID_FIELD).toJson());
        if (isCopying.get()) {
          sourceOffset.put(COPY_KEY, "true");
        }

        String topicName = topicMapper.getTopic(changeStreamDocument);
        if (topicName.isEmpty()) {
          LOGGER.warn(
              "No topic set. Could not publish the message: {}", changeStreamDocument.toJson());
          return sourceRecords;
        }

        Optional<BsonDocument> valueDocument = Optional.empty();
        if (publishFullDocumentOnly) {
          if (changeStreamDocument.containsKey(FULL_DOCUMENT)
              && changeStreamDocument.get(FULL_DOCUMENT).isDocument()) {
            valueDocument = Optional.of(changeStreamDocument.getDocument(FULL_DOCUMENT));
          }
        } else {
          valueDocument = Optional.of(changeStreamDocument);
        }

        valueDocument.ifPresent(
            (valueDoc) -> {
              LOGGER.trace("Adding {} to {}: {}", valueDoc, topicName, sourceOffset);

              BsonDocument keyDocument =
                  sourceConfig.getKeyOutputFormat() == OutputFormat.SCHEMA
                      ? changeStreamDocument
                      : new BsonDocument(ID_FIELD, changeStreamDocument.get(ID_FIELD));

              createSourceRecord(
                      partition,
                      keySchemaAndValueProducer,
                      valueSchemaAndValueProducer,
                      sourceOffset,
                      topicName,
                      keyDocument,
                      valueDoc)
                  .map(sourceRecords::add);
            });

        if (sourceRecords.size() == maxBatchSize) {
          LOGGER.debug(
              "Reached '{}': {}, returning records", POLL_MAX_BATCH_SIZE_CONFIG, maxBatchSize);
          return sourceRecords;
        }
      }
    }
    return null;
  }

  private Optional<SourceRecord> createSourceRecord(
      final Map<String, Object> partition,
      final SchemaAndValueProducer keySchemaAndValueProducer,
      final SchemaAndValueProducer valueSchemaAndValueProducer,
      final Map<String, String> sourceOffset,
      final String topicName,
      final BsonDocument keyDocument,
      final BsonDocument valueDocument) {

    try {
      SchemaAndValue keySchemaAndValue = keySchemaAndValueProducer.get(keyDocument);
      SchemaAndValue valueSchemaAndValue = valueSchemaAndValueProducer.get(valueDocument);
      return Optional.of(
          new SourceRecord(
              partition,
              sourceOffset,
              topicName,
              keySchemaAndValue.schema(),
              keySchemaAndValue.value(),
              valueSchemaAndValue.schema(),
              valueSchemaAndValue.value()));
    } catch (Exception e) {
      Supplier<String> errorMessage =
          () ->
              format(
                  "Exception creating Source record for: Key=%s Value=%s",
                  keyDocument.toJson(), valueDocument.toJson());
      if (sourceConfig.logErrors()) {
        LOGGER.error(errorMessage.get(), e);
      }
      if (sourceConfig.tolerateErrors()) {
        if (sourceConfig.getString(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG).isEmpty()) {
          return Optional.empty();
        }
        return Optional.of(
            new SourceRecord(
                partition,
                sourceOffset,
                sourceConfig.getString(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG),
                Schema.STRING_SCHEMA,
                keyDocument.toJson(),
                Schema.STRING_SCHEMA,
                valueDocument.toJson()));
      }
      throw new DataException(errorMessage.get(), e);
    }
  }

  @Override
  public synchronized void stop() {
    // Synchronized because polling blocks and stop can be called from another thread
    LOGGER.info("Stopping MongoDB source task");
    isRunning.set(false);
    isCopying.set(false);
    if (copyDataManager != null) {
      copyDataManager.close();
      copyDataManager = null;
    }
    if (cursor != null) {
      cursor.close();
      cursor = null;
    }
    if (mongoClient != null) {
      mongoClient.close();
      mongoClient = null;
    }
    supportsStartAfter = true;
    invalidatedCursor = false;
  }

  void initializeCursorAndHeartbeatManager(
      final Time time, final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {
    cursor = createCursor(sourceConfig, mongoClient);
    heartbeatManager =
        new HeartbeatManager(
            time,
            cursor,
            sourceConfig.getLong(HEARTBEAT_INTERVAL_MS_CONFIG),
            sourceConfig.getString(HEARTBEAT_TOPIC_NAME_CONFIG),
            partitionMap);
  }

  MongoChangeStreamCursor<? extends BsonDocument> createCursor(
      final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {
    LOGGER.debug("Creating a MongoCursor");
    return tryCreateCursor(sourceConfig, mongoClient, getResumeToken(sourceConfig));
  }

  private MongoChangeStreamCursor<? extends BsonDocument> tryCreateCursor(
      final MongoSourceConfig sourceConfig,
      final MongoClient mongoClient,
      final BsonDocument resumeToken) {
    try {
      ChangeStreamIterable<Document> changeStreamIterable =
          getChangeStreamIterable(sourceConfig, mongoClient);
      if (resumeToken != null && supportsStartAfter) {
        LOGGER.info("Resuming the change stream after the previous offset: {}", resumeToken);
        changeStreamIterable.startAfter(resumeToken);
      } else if (resumeToken != null && !invalidatedCursor) {
        LOGGER.info(
            "Resuming the change stream after the previous offset using resumeAfter: {}",
            resumeToken);
        changeStreamIterable.resumeAfter(resumeToken);
      } else {
        LOGGER.info("New change stream cursor created without offset.");
      }
      return (MongoChangeStreamCursor<BsonDocument>)
          changeStreamIterable.withDocumentClass(BsonDocument.class).cursor();
    } catch (MongoCommandException e) {
      if (resumeToken != null) {
        if (invalidatedResumeToken(e)) {
          invalidatedCursor = true;
          return tryCreateCursor(sourceConfig, mongoClient, null);
        } else if (doesNotSupportsStartAfter(e)) {
          supportsStartAfter = false;
          return tryCreateCursor(sourceConfig, mongoClient, resumeToken);
        } else if (sourceConfig.tolerateErrors() && resumeTokenNotFound(e)) {
          LOGGER.warn(
              "Failed to resume change stream: {} {}\n"
                  + "===================================================================================\n"
                  + "When the resume token is no longer available there is the potential for data loss.\n\n"
                  + "Restarting the change stream with no resume token because `errors.tolerance=all`.\n"
                  + "===================================================================================\n",
              e.getErrorMessage(),
              e.getErrorCode());
          invalidatedCursor = true;
          return tryCreateCursor(sourceConfig, mongoClient, null);
        }
      }
      if (e.getErrorCode() == NAMESPACE_NOT_FOUND_ERROR) {
        LOGGER.info("Namespace not found cursor closed.");
      } else {
        LOGGER.warn(
            "Failed to resume change stream: {} {}\n\n"
                + "=====================================================================================\n"
                + "If the resume token is no longer available then there is the potential for data loss.\n"
                + "Saved resume tokens are managed by Kafka and stored with the offset data.\n\n"
                + "To restart the change stream with no resume token either: \n"
                + "  * Create a new partition name using the `offset.partition.name` configuration.\n"
                + "  * Set `errors.tolerance=all` and ignore the erroring resume token. \n"
                + "  * Manually remove the old offset from its configured storage.\n\n"
                + "Resetting the offset will allow for the connector to be resume from the latest resume\n"
                + "token. Using `copy.existing=true` ensures that all data will be outputted by the\n"
                + "connector but it will duplicate existing data.\n"
                + "=====================================================================================\n",
            e.getErrorMessage(),
            e.getErrorCode());
        if (resumeTokenNotFound(e)) {
          throw new ConnectException(
              "ResumeToken not found. Cannot create a change stream cursor", e);
        }
      }
      return null;
    }
  }

  private boolean doesNotSupportsStartAfter(final MongoCommandException e) {
    return ((e.getErrorCode() == FAILED_TO_PARSE_ERROR || e.getErrorCode() == UNKNOWN_FIELD_ERROR)
        && e.getErrorMessage().contains("startAfter"));
  }

  private boolean invalidatedResumeToken(final MongoCommandException e) {
    return e.getErrorCode() == INVALIDATED_RESUME_TOKEN_ERROR;
  }

  private boolean resumeTokenNotFound(final MongoCommandException e) {
    String errorMessage = e.getErrorMessage().toLowerCase(Locale.ROOT);
    return errorMessage.contains(RESUME_TOKEN)
        && (errorMessage.contains(NOT_FOUND)
            || errorMessage.contains(DOES_NOT_EXIST)
            || errorMessage.contains(INVALID_RESUME_TOKEN));
  }

  Map<String, Object> createPartitionMap(final MongoSourceConfig sourceConfig) {
    if (partitionMap == null) {
      String partitionName = sourceConfig.getString(MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG);
      if (partitionName.isEmpty()) {
        partitionName = createDefaultPartitionName(sourceConfig);
      }
      partitionMap = singletonMap(NS_KEY, partitionName);
    }
    return partitionMap;
  }

  Map<String, Object> createLegacyPartitionMap(final MongoSourceConfig sourceConfig) {
    return singletonMap(NS_KEY, createLegacyPartitionName(sourceConfig));
  }

  String createLegacyPartitionName(final MongoSourceConfig sourceConfig) {
    return format(
        "%s/%s.%s",
        sourceConfig.getString(CONNECTION_URI_CONFIG),
        sourceConfig.getString(DATABASE_CONFIG),
        sourceConfig.getString(COLLECTION_CONFIG));
  }

  String createDefaultPartitionName(final MongoSourceConfig sourceConfig) {
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
  private boolean shouldCopyData() {
    Map<String, Object> offset = getOffset(sourceConfig);
    return sourceConfig.getBoolean(COPY_EXISTING_CONFIG)
        && (offset == null || offset.containsKey(COPY_KEY));
  }

  /**
   * This method also is responsible for caching the {@code resumeAfter} value for the change
   * stream.
   */
  private void setCachedResultAndResumeToken() {
    MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStreamCursor;

    try {
      changeStreamCursor = getChangeStreamIterable(sourceConfig, mongoClient).cursor();
    } catch (MongoCommandException e) {
      if (e.getErrorCode() == NAMESPACE_NOT_FOUND_ERROR) {
        return;
      }
      throw new ConnectException(e);
    }
    ChangeStreamDocument<Document> firstResult = changeStreamCursor.tryNext();
    if (firstResult != null) {
      cachedResult =
          new BsonDocumentWrapper<>(
              firstResult,
              ChangeStreamDocument.createCodec(
                  Document.class, MongoClientSettings.getDefaultCodecRegistry()));
    }
    cachedResumeToken =
        firstResult != null ? firstResult.getResumeToken() : changeStreamCursor.getResumeToken();
    changeStreamCursor.close();
  }

  /**
   * Returns the next document to be delivered to Kafka.
   *
   * <p>
   *
   * <ol>
   *   <li>If copying data is in progress, returns the next result.
   *   <li>If copying data and all data has been copied and there is a cached result return the
   *       cached result.
   *   <li>Otherwise, return the next result from the change stream cursor. Creating a new cursor if
   *       necessary.
   * </ol>
   *
   * @return the next document
   */
  private Optional<BsonDocument> getNextDocument() {
    if (isCopying.get()) {
      Optional<BsonDocument> result = copyDataManager.poll();
      if (result.isPresent() || copyDataManager.isCopying()) {
        return result;
      }

      // No longer copying
      LOGGER.info("Shutting down executors");
      isCopying.set(false);
      if (cachedResult != null) {
        result = Optional.of(cachedResult);
        cachedResult = null;
        return result;
      }
      LOGGER.info("Finished copying existing data from the collection(s).");
    }

    if (cursor == null) {
      initializeCursorAndHeartbeatManager(time, sourceConfig, mongoClient);
    }

    if (cursor != null) {
      try {
        BsonDocument next = cursor.tryNext();
        // The cursor has been closed by the server
        if (next == null && cursor.getServerCursor() == null) {
          invalidateCursorAndReinitialize();
          next = cursor != null ? cursor.tryNext() : null;
        }
        return Optional.ofNullable(next);
      } catch (Exception e) {
        if (cursor != null) {
          cursor.close();
          cursor = null;
        }
        if (isRunning.get()) {
          LOGGER.info(
              "An exception occurred when trying to get the next item from the Change Stream: {}",
              e.getMessage());
        }
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  private void invalidateCursorAndReinitialize() {
    invalidatedCursor = true;
    cursor.close();
    cursor = null;
    initializeCursorAndHeartbeatManager(time, sourceConfig, mongoClient);
  }

  private ChangeStreamIterable<Document> getChangeStreamIterable(
      final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {
    String database = sourceConfig.getString(DATABASE_CONFIG);
    String collection = sourceConfig.getString(COLLECTION_CONFIG);

    Optional<List<Document>> pipeline = sourceConfig.getPipeline();
    ChangeStreamIterable<Document> changeStream;
    if (database.isEmpty()) {
      LOGGER.info("Watching all changes on the cluster");
      changeStream = pipeline.map(mongoClient::watch).orElse(mongoClient.watch());
    } else if (collection.isEmpty()) {
      LOGGER.info("Watching for database changes on '{}'", database);
      MongoDatabase db = mongoClient.getDatabase(database);
      changeStream = pipeline.map(db::watch).orElse(db.watch());
    } else {
      LOGGER.info("Watching for collection changes on '{}.{}'", database, collection);
      MongoCollection<Document> coll = mongoClient.getDatabase(database).getCollection(collection);
      changeStream = pipeline.map(coll::watch).orElse(coll.watch());
    }

    int batchSize = sourceConfig.getInt(BATCH_SIZE_CONFIG);
    if (batchSize > 0) {
      changeStream.batchSize(batchSize);
    }
    sourceConfig.getFullDocument().ifPresent(changeStream::fullDocument);
    sourceConfig.getCollation().ifPresent(changeStream::collation);
    return changeStream;
  }

  Map<String, Object> getOffset(final MongoSourceConfig sourceConfig) {
    if (context != null) {
      Map<String, Object> offset =
          context.offsetStorageReader().offset(createPartitionMap(sourceConfig));
      if (offset == null
          && sourceConfig.getString(MongoSourceConfig.OFFSET_PARTITION_NAME_CONFIG).isEmpty()) {
        offset = context.offsetStorageReader().offset(createLegacyPartitionMap(sourceConfig));
      }
      return offset;
    }
    return null;
  }

  BsonDocument getResumeToken(final MongoSourceConfig sourceConfig) {
    BsonDocument resumeToken = null;
    if (cachedResumeToken != null) {
      resumeToken = cachedResumeToken;
      cachedResumeToken = null;
    } else if (invalidatedCursor) {
      invalidatedCursor = false;
    } else {
      Map<String, Object> offset = getOffset(sourceConfig);
      if (offset != null && offset.containsKey(ID_FIELD) && !offset.containsKey(COPY_KEY)) {
        resumeToken = BsonDocument.parse((String) offset.get(ID_FIELD));
        if (offset.containsKey(HEARTBEAT_KEY)) {
          LOGGER.info("Resume token from heartbeat: {}", resumeToken);
        }
      }
    }
    return resumeToken;
  }
}
