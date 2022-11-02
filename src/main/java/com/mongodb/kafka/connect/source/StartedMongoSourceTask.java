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
package com.mongodb.kafka.connect.source;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceTask.COPY_KEY;
import static com.mongodb.kafka.connect.source.MongoSourceTask.ID_FIELD;
import static com.mongodb.kafka.connect.source.MongoSourceTask.LOGGER;
import static com.mongodb.kafka.connect.source.MongoSourceTask.createPartitionMap;
import static com.mongodb.kafka.connect.source.MongoSourceTask.doesNotSupportsStartAfter;
import static com.mongodb.kafka.connect.source.MongoSourceTask.getOffset;
import static com.mongodb.kafka.connect.source.heartbeat.HeartbeatManager.HEARTBEAT_KEY;
import static com.mongodb.kafka.connect.source.producer.SchemaAndValueProducers.createKeySchemaAndValueProvider;
import static com.mongodb.kafka.connect.source.producer.SchemaAndValueProducers.createValueSchemaAndValueProvider;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.lang.Nullable;

import com.mongodb.kafka.connect.source.heartbeat.HeartbeatManager;
import com.mongodb.kafka.connect.source.producer.SchemaAndValueProducer;
import com.mongodb.kafka.connect.source.statistics.StatisticsManager;
import com.mongodb.kafka.connect.source.topic.mapping.TopicMapper;
import com.mongodb.kafka.connect.util.VisibleForTesting;
import com.mongodb.kafka.connect.util.jmx.SourceTaskStatistics;
import com.mongodb.kafka.connect.util.time.InnerOuterTimer;
import com.mongodb.kafka.connect.util.time.InnerOuterTimer.InnerTimer;

final class StartedMongoSourceTask implements AutoCloseable {
  private static final String FULL_DOCUMENT = "fullDocument";
  private static final int NAMESPACE_NOT_FOUND_ERROR = 26;
  private static final int ILLEGAL_OPERATION_ERROR = 20;
  private static final int INVALIDATED_RESUME_TOKEN_ERROR = 260;
  private static final int CHANGE_STREAM_FATAL_ERROR = 280;
  private static final int CHANGE_STREAM_HISTORY_LOST = 286;
  private static final int BSON_OBJECT_TOO_LARGE = 10334;
  private static final Set<Integer> INVALID_CHANGE_STREAM_ERRORS =
      new HashSet<>(
          asList(
              INVALIDATED_RESUME_TOKEN_ERROR,
              CHANGE_STREAM_FATAL_ERROR,
              CHANGE_STREAM_HISTORY_LOST,
              BSON_OBJECT_TOO_LARGE));
  private static final String RESUME_TOKEN = "resume token";
  private static final String RESUME_POINT = "resume point";
  private static final String NOT_FOUND = "not found";
  private static final String DOES_NOT_EXIST = "does not exist";
  private static final String INVALID_RESUME_TOKEN = "invalid resume token";
  private static final String NO_LONGER_IN_THE_OPLOG = "no longer be in the oplog";

  private final Supplier<SourceTaskContext> sourceTaskContextAccessor;
  private final Time time;
  private volatile boolean isRunning;
  private boolean isCopying;

  private final MongoSourceConfig sourceConfig;
  private final Map<String, Object> partitionMap;
  private final MongoClient mongoClient;
  private HeartbeatManager heartbeatManager;

  private boolean supportsStartAfter = true;
  private boolean invalidatedCursor = false;
  @Nullable private final MongoCopyDataManager copyDataManager;
  private BsonDocument cachedResult;
  private BsonDocument cachedResumeToken;

  private MongoChangeStreamCursor<? extends BsonDocument> cursor;
  private final StatisticsManager statisticsManager;
  private final InnerOuterTimer inTaskPollInConnectFrameworkTimer;

  StartedMongoSourceTask(
      final Supplier<SourceTaskContext> sourceTaskContextAccessor,
      final MongoSourceConfig sourceConfig,
      final MongoClient mongoClient,
      @Nullable final MongoCopyDataManager copyDataManager,
      final StatisticsManager statisticsManager) {
    this.sourceTaskContextAccessor = sourceTaskContextAccessor;
    this.sourceConfig = sourceConfig;
    this.mongoClient = mongoClient;
    isRunning = true;
    boolean shouldCopyData = copyDataManager != null;
    isCopying = shouldCopyData;
    time = new SystemTime();
    partitionMap = createPartitionMap(sourceConfig);
    this.copyDataManager = copyDataManager;
    if (shouldCopyData) {
      setCachedResultAndResumeToken();
    } else {
      initializeCursorAndHeartbeatManager();
    }
    this.statisticsManager = statisticsManager;
    inTaskPollInConnectFrameworkTimer =
        InnerOuterTimer.start(
            (inTaskPollSample) -> {
              SourceTaskStatistics statistics = statisticsManager.currentStatistics();
              statistics.getInTaskPoll().sample(inTaskPollSample.toMillis());
              if (LOGGER.isDebugEnabled()) {
                // toJSON relatively expensive
                LOGGER.debug(statistics.getName() + ": " + statistics.toJSON());
              }
            },
            (inFrameworkSample) ->
                statisticsManager
                    .currentStatistics()
                    .getInConnectFramework()
                    .sample(inFrameworkSample.toMillis()));
  }

  /** @see MongoSourceTask#poll() */
  @SuppressWarnings("try")
  List<SourceRecord> poll() {
    if (!isCopying) {
      statisticsManager.switchToStreamStatistics();
    }
    try (InnerTimer automatic = inTaskPollInConnectFrameworkTimer.sampleOuter()) {
      List<SourceRecord> sourceRecords = pollInternal();
      if (sourceRecords != null) {
        statisticsManager.currentStatistics().getRecords().sample(sourceRecords.size());
      }
      return sourceRecords;
    }
  }

  private List<SourceRecord> pollInternal() {
    final long startPoll = time.milliseconds();
    LOGGER.debug("Polling Start: {}", startPoll);
    List<SourceRecord> sourceRecords = new ArrayList<>();
    TopicMapper topicMapper = sourceConfig.getTopicMapper();
    boolean publishFullDocumentOnly = sourceConfig.getBoolean(PUBLISH_FULL_DOCUMENT_ONLY_CONFIG);
    int maxBatchSize = sourceConfig.getInt(POLL_MAX_BATCH_SIZE_CONFIG);
    long nextUpdate = startPoll + sourceConfig.getLong(POLL_AWAIT_TIME_MS_CONFIG);

    SchemaAndValueProducer keySchemaAndValueProducer =
        createKeySchemaAndValueProvider(sourceConfig);
    SchemaAndValueProducer valueSchemaAndValueProducer =
        createValueSchemaAndValueProvider(sourceConfig);

    while (isRunning) {
      Optional<BsonDocument> next = getNextDocument();
      long untilNext = nextUpdate - time.milliseconds();

      if (!next.isPresent()) {
        if (untilNext > 0) {
          LOGGER.debug("Waiting {} ms to poll", untilNext);
          time.sleep(untilNext);
          continue; // Re-check stop flag before continuing
        }
        if (!sourceRecords.isEmpty()) {
          LOGGER.debug("Returning {} source records", sourceRecords.size());
          return sourceRecords;
        }
        if (heartbeatManager != null) {
          Optional<SourceRecord> heartbeat = heartbeatManager.heartbeat();
          if (heartbeat.isPresent()) {
            LOGGER.debug("Returning single heartbeat record");
            return singletonList(heartbeat.get());
          } else {
            return null;
          }
        }
        LOGGER.debug("Returning null because there are no source records and no heartbeat manager");
        return null;
      } else {
        BsonDocument changeStreamDocument = next.get();

        Map<String, String> sourceOffset = new HashMap<>();
        sourceOffset.put(ID_FIELD, changeStreamDocument.getDocument(ID_FIELD).toJson());
        if (isCopying) {
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
            (BsonDocument valueDoc) -> {
              LOGGER.trace("Adding {} to {}: {}", valueDoc, topicName, sourceOffset);

              if (valueDoc instanceof RawBsonDocument) {
                int sizeBytes = ((RawBsonDocument) valueDoc).getByteBuffer().limit();
                statisticsManager.currentStatistics().getMongodbBytesRead().sample(sizeBytes);
              }

              BsonDocument keyDocument =
                  sourceConfig.getKeyOutputFormat() == MongoSourceConfig.OutputFormat.SCHEMA
                      ? changeStreamDocument
                      : new BsonDocument(ID_FIELD, changeStreamDocument.get(ID_FIELD));

              createSourceRecord(
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
        } else {
          LOGGER.debug(
              "Continuing to loop because sourceRecords size {} is less than maxBatchSize {}",
              sourceRecords.size(),
              maxBatchSize);
        }
      }
    }
    LOGGER.debug("Returning null because connector is no longer running");
    return null;
  }

  private Optional<SourceRecord> createSourceRecord(
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
              partitionMap,
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
                  "%s : Exception creating Source record for: Key=%s Value=%s",
                  e.getMessage(), keyDocument.toJson(), valueDocument.toJson());
      if (sourceConfig.logErrors()) {
        LOGGER.error(errorMessage.get(), e);
      }
      if (sourceConfig.tolerateErrors()) {
        if (sourceConfig.getDlqTopic().isEmpty()) {
          return Optional.empty();
        }
        return Optional.of(
            new SourceRecord(
                partitionMap,
                sourceOffset,
                sourceConfig.getDlqTopic(),
                Schema.STRING_SCHEMA,
                keyDocument.toJson(),
                Schema.STRING_SCHEMA,
                valueDocument.toJson()));
      }
      throw new DataException(errorMessage.get(), e);
    }
  }

  /** @see MongoSourceTask#stop() */
  @SuppressWarnings("try")
  @Override
  public void close() {
    LOGGER.info("Stopping MongoDB source task");
    isRunning = false;

    //noinspection EmptyTryBlock
    try (StatisticsManager autoCloseable4 = this.statisticsManager;
        MongoClient autoCloseable3 = this.mongoClient;
        MongoChangeStreamCursor<? extends BsonDocument> autoCloseable2 = this.cursor;
        MongoCopyDataManager autoCloseable1 = this.copyDataManager) {
      // just using try-with-resources to ensure they all get closed, even in the case of exceptions
    }
  }

  private void initializeCursorAndHeartbeatManager() {
    cursor = createCursor(sourceConfig, mongoClient);
    heartbeatManager =
        new HeartbeatManager(
            time,
            cursor,
            sourceConfig.getLong(HEARTBEAT_INTERVAL_MS_CONFIG),
            sourceConfig.getString(HEARTBEAT_TOPIC_NAME_CONFIG),
            partitionMap);
  }

  @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
  MongoChangeStreamCursor<? extends BsonDocument> createCursor(
      final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {
    LOGGER.debug("Creating a MongoCursor");
    return tryCreateCursor(sourceConfig, mongoClient, getResumeToken(sourceConfig));
  }

  private MongoChangeStreamCursor<? extends BsonDocument> tryRecreateCursor(
      final MongoException e) {
    int errorCode =
        e instanceof MongoCommandException
            ? ((MongoCommandException) e).getErrorCode()
            : e.getCode();
    String errorMessage =
        e instanceof MongoCommandException
            ? ((MongoCommandException) e).getErrorMessage()
            : e.getMessage();
    LOGGER.warn(
        "Failed to resume change stream: {} {}\n"
            + "===================================================================================\n"
            + "When the resume token is no longer available there is the potential for data loss.\n\n"
            + "Restarting the change stream with no resume token because `errors.tolerance=all`.\n"
            + "===================================================================================\n",
        errorMessage,
        errorCode);
    invalidatedCursor = true;
    return tryCreateCursor(sourceConfig, mongoClient, null);
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
      return (MongoChangeStreamCursor<RawBsonDocument>)
          changeStreamIterable.withDocumentClass(RawBsonDocument.class).cursor();
    } catch (MongoCommandException e) {
      if (resumeToken != null) {
        if (invalidatedResumeToken(e)) {
          invalidatedCursor = true;
          return tryCreateCursor(sourceConfig, mongoClient, null);
        } else if (doesNotSupportsStartAfter(e)) {
          supportsStartAfter = false;
          return tryCreateCursor(sourceConfig, mongoClient, resumeToken);
        } else if (sourceConfig.tolerateErrors() && changeStreamNotValid(e)) {
          return tryRecreateCursor(e);
        }
      }
      if (e.getErrorCode() == NAMESPACE_NOT_FOUND_ERROR) {
        LOGGER.info("Namespace not found cursor closed.");
      } else if (e.getErrorCode() == ILLEGAL_OPERATION_ERROR) {
        LOGGER.warn(
            "Illegal $changeStream operation: {} {}\n\n"
                + "=====================================================================================\n"
                + "{}\n\n"
                + "Please Note: Not all aggregation pipeline operations are suitable for modifying the\n"
                + "change stream output. For more information, please see the official documentation:\n"
                + "   https://docs.mongodb.com/manual/changeStreams/\n"
                + "=====================================================================================\n",
            e.getErrorMessage(),
            e.getErrorCode(),
            e.getErrorMessage());
        throw new ConnectException("Illegal $changeStream operation", e);
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
        if (changeStreamNotValid(e)) {
          throw new ConnectException(
              "ResumeToken not found. Cannot create a change stream cursor", e);
        }
      }
      return null;
    }
  }

  private static boolean invalidatedResumeToken(final MongoCommandException e) {
    return e.getErrorCode() == INVALIDATED_RESUME_TOKEN_ERROR;
  }

  private static boolean changeStreamNotValid(final MongoException e) {
    if (INVALID_CHANGE_STREAM_ERRORS.contains(e.getCode())) {
      return true;
    }
    String errorMessage =
        e instanceof MongoCommandException
            ? ((MongoCommandException) e).getErrorMessage().toLowerCase(Locale.ROOT)
            : e.getMessage().toLowerCase(Locale.ROOT);
    return (errorMessage.contains(RESUME_TOKEN) || errorMessage.contains(RESUME_POINT))
        && (errorMessage.contains(NOT_FOUND)
            || errorMessage.contains(DOES_NOT_EXIST)
            || errorMessage.contains(INVALID_RESUME_TOKEN)
            || errorMessage.contains(NO_LONGER_IN_THE_OPLOG));
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
    if (isCopying) {
      Optional<BsonDocument> result = copyDataManager.poll();
      if (result.isPresent() || copyDataManager.isCopying()) {
        return result;
      }

      isCopying = false;
      LOGGER.info("Finished copying existing data from the collection(s).");
      if (cachedResult != null) {
        result = Optional.of(cachedResult);
        cachedResult = null;
        return result;
      }
    }

    if (cursor == null) {
      initializeCursorAndHeartbeatManager();
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
      } catch (MongoException e) {
        closeCursor();
        if (isRunning) {
          if (sourceConfig.tolerateErrors() && changeStreamNotValid(e)) {
            cursor = tryRecreateCursor(e);
          } else {
            LOGGER.info(
                "An exception occurred when trying to get the next item from the Change Stream", e);
          }
        }
        return Optional.empty();
      } catch (Exception e) {
        closeCursor();
        if (isRunning) {
          throw new ConnectException("Unexpected error: " + e.getMessage(), e);
        }
      }
    }
    return Optional.empty();
  }

  private void closeCursor() {
    if (cursor != null) {
      try {
        cursor.close();
      } catch (Exception e1) {
        // ignore
      }
      cursor = null;
    }
  }

  private void invalidateCursorAndReinitialize() {
    invalidatedCursor = true;
    cursor.close();
    cursor = null;
    initializeCursorAndHeartbeatManager();
  }

  private static ChangeStreamIterable<Document> getChangeStreamIterable(
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
    sourceConfig.getFullDocumentBeforeChange().ifPresent(changeStream::fullDocumentBeforeChange);
    sourceConfig.getFullDocument().ifPresent(changeStream::fullDocument);
    sourceConfig.getCollation().ifPresent(changeStream::collation);
    return changeStream;
  }

  private BsonDocument getResumeToken(final MongoSourceConfig sourceConfig) {
    BsonDocument resumeToken = null;
    if (cachedResumeToken != null) {
      resumeToken = cachedResumeToken;
      cachedResumeToken = null;
    } else if (invalidatedCursor) {
      invalidatedCursor = false;
    } else {
      Map<String, Object> offset = getOffset(sourceTaskContextAccessor.get(), sourceConfig);
      if (offset != null && offset.containsKey(ID_FIELD) && !offset.containsKey(COPY_KEY)) {
        resumeToken = BsonDocument.parse((String) offset.get(ID_FIELD));
        if (offset.containsKey(HEARTBEAT_KEY)) {
          LOGGER.info("Resume token from heartbeat: {}", resumeToken);
        }
      }
    }
    return resumeToken;
  }

  /** @see MongoSourceTask#commitRecord(SourceRecord, RecordMetadata) */
  void commitRecord(final SourceRecord record, final RecordMetadata metadata) {
    if (metadata == null) {
      statisticsManager.currentStatistics().getRecordsFiltered().sample(1);
    } else {
      statisticsManager.currentStatistics().getRecordsAcknowledged().sample(1);
    }
  }
}
