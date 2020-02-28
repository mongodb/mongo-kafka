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
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;

import com.mongodb.kafka.connect.Versions;

/**
 * A Kafka Connect source task that uses change streams to broadcast changes to the collection, database or client.
 *
 * <h2>Copy Existing Data</h2>
 * <p>
 * If configured the connector will copy the existing data from the collection, database or client. All namespaces that
 * exist at the time of starting the task will be broadcast onto the topic as insert operations. Only when all the data
 * from all namespaces have been broadcast will the change stream cursor start broadcasting new changes. The logic for
 * copying existing data is as follows:
 * <ol>
 * <li>Get the latest resumeToken from MongoDB</li>
 * <li>Create insert events for all configured namespaces using multiple threads. This step is completed only
 * after <em>all</em> collections are successfully copied.</li>
 * <li>Start a change stream cursor from the saved resumeToken</li>
 * </ol>
 * <p>
 * It should be noted that the reading of all the data during the copy and then the subsequent change stream events
 * may produce duplicated events. During the copy, clients can make changes to the data in MongoDB, which
 * may be represented both by the copying process and the change stream. However, as the change stream events are
 * idempotent the changes can be applied so that the data is eventually consistent.
 * </p>
 * <p>It should also be noted renaming a collection during the copying process is not supported.</p>
 *
 * <h3>Restarts</h3>
 * Restarting the connector during the copying phase, will cause the whole copy process to restart.
 * Restarts after the copying process will resume from the last seen resumeToken.
 */
public class MongoSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSourceTask.class);

    private final Time time;
    private final AtomicBoolean isRunning = new AtomicBoolean();
    private final AtomicBoolean isCopying = new AtomicBoolean();

    private MongoSourceConfig sourceConfig;
    private MongoClient mongoClient;

    private boolean supportsStartAfter = true;
    private boolean invalidatedCursor = false;
    private MongoCopyDataManager copyDataManager;
    private BsonDocument cachedResult;
    private BsonDocument cachedResumeToken;

    private MongoCursor<BsonDocument> cursor;

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
        LOGGER.debug("Starting MongoDB source task");
        try {
            sourceConfig = new MongoSourceConfig(props);
        } catch (Exception e) {
            throw new ConnectException("Failed to start new task", e);
        }

        mongoClient = MongoClients.create(sourceConfig.getConnectionString(), getMongoDriverInformation());
        if (shouldCopyData()) {
            setCachedResultAndResumeToken();
            copyDataManager = new MongoCopyDataManager(sourceConfig, mongoClient);
            isCopying.set(true);
        } else {
            cursor = createCursor(sourceConfig, mongoClient);
        }
        isRunning.set(true);
        LOGGER.debug("Started MongoDB source task");
    }

    @Override
    public List<SourceRecord> poll() {
        final long startPoll = time.milliseconds();
        LOGGER.debug("Polling Start: {}", time.milliseconds());
        List<SourceRecord> sourceRecords = new ArrayList<>();
        boolean publishFullDocumentOnly = sourceConfig.getBoolean(PUBLISH_FULL_DOCUMENT_ONLY_CONFIG);
        int maxBatchSize = sourceConfig.getInt(POLL_MAX_BATCH_SIZE_CONFIG);
        long nextUpdate = startPoll + sourceConfig.getLong(POLL_AWAIT_TIME_MS_CONFIG);
        String prefix = sourceConfig.getString(TOPIC_PREFIX_CONFIG);
        Map<String, Object> partition = createPartitionMap(sourceConfig);

        while (isRunning.get()) {
            Optional<BsonDocument> next = getNextDocument();
            long untilNext = nextUpdate - time.milliseconds();

            if (!next.isPresent()) {
                if (untilNext > 0) {
                    LOGGER.debug("Waiting {} ms to poll", untilNext);
                    time.sleep(untilNext);
                    continue; // Re-check stop flag before continuing
                }
                LOGGER.debug("Poll await time passed before reaching max batch size returning {} records", sourceRecords.size());
                return sourceRecords.isEmpty() ? null : sourceRecords;
            } else {
                BsonDocument changeStreamDocument = next.get();

                Map<String, String> sourceOffset = new HashMap<>();
                sourceOffset.put("_id", changeStreamDocument.getDocument("_id").toJson());
                if (isCopying.get()) {
                    sourceOffset.put("copy", "true");
                }

                String topicName = getTopicNameFromNamespace(prefix, changeStreamDocument.getDocument("ns", new BsonDocument()));

                Optional<String> jsonDocument = Optional.empty();
                if (publishFullDocumentOnly) {
                    if (changeStreamDocument.containsKey("fullDocument")) {
                        jsonDocument = Optional.of(changeStreamDocument.getDocument("fullDocument").toJson());
                    }
                } else {
                    jsonDocument = Optional.of(changeStreamDocument.toJson());
                }

                jsonDocument.ifPresent((json) -> {
                    LOGGER.trace("Adding {} to {}: {}", json, topicName, sourceOffset);
                    String keyJson = new BsonDocument("_id", changeStreamDocument.get("_id")).toJson();
                    sourceRecords.add(new SourceRecord(partition, sourceOffset, topicName, Schema.STRING_SCHEMA, keyJson,
                            Schema.STRING_SCHEMA, json));
                });

                if (sourceRecords.size() == maxBatchSize) {
                    LOGGER.debug("Reached max batch size: {}, returning records", maxBatchSize);
                    return sourceRecords;
                }
            }
        }
        return null;
    }

    @Override
    public synchronized void stop() {
        // Synchronized because polling blocks and stop can be called from another thread
        LOGGER.debug("Stopping MongoDB source task");
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

    MongoCursor<BsonDocument> createCursor(final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {
        LOGGER.debug("Creating a MongoCursor");
        return tryCreateCursor(sourceConfig, mongoClient, getResumeToken(sourceConfig));
    }

    private MongoCursor<BsonDocument> tryCreateCursor(final MongoSourceConfig sourceConfig, final MongoClient mongoClient,
                                                      final BsonDocument resumeToken) {
        try {
            ChangeStreamIterable<Document> changeStreamIterable = getChangeStreamIterable(sourceConfig, mongoClient);
            if (resumeToken != null && supportsStartAfter) {
                LOGGER.info("Resuming the change stream after the previous offset");
                changeStreamIterable.startAfter(resumeToken);
            } else if (resumeToken != null && !invalidatedCursor) {
                LOGGER.info("Resuming the change stream after the previous offset using resumeAfter");
                changeStreamIterable.resumeAfter(resumeToken);
            } else {
                LOGGER.info("New change stream cursor created without offset");
            }
            return changeStreamIterable.withDocumentClass(BsonDocument.class).iterator();
        } catch (MongoCommandException e) {
            if (resumeToken != null) {
                if (e.getErrorCode() == 260) {
                    invalidatedCursor = true;
                    return tryCreateCursor(sourceConfig, mongoClient, null);
                } else if (e.getErrorCode() == 40415 && e.getErrorMessage().contains("startAfter")) {
                    supportsStartAfter = false;
                    return tryCreateCursor(sourceConfig, mongoClient, resumeToken);
                }
            }
            LOGGER.info("Failed to resume change stream: {} {}", e.getErrorMessage(), e.getErrorCode());
            return null;
        }
    }

    String getTopicNameFromNamespace(final String prefix, final BsonDocument namespaceDocument) {
        String topicName = "";
        if (namespaceDocument.containsKey("db")) {
            topicName = namespaceDocument.getString("db").getValue();
            if (namespaceDocument.containsKey("coll")) {
                topicName = format("%s.%s", topicName, namespaceDocument.getString("coll").getValue());
            }
        }
        return prefix.isEmpty() ? topicName : format("%s.%s", prefix, topicName);
    }

    Map<String, Object> createPartitionMap(final MongoSourceConfig sourceConfig) {
        return singletonMap("ns", format("%s/%s.%s", sourceConfig.getString(CONNECTION_URI_CONFIG),
                sourceConfig.getString(DATABASE_CONFIG), sourceConfig.getString(COLLECTION_CONFIG)));
    }

    /**
     * Checks to see if data should be copied.
     *
     * <p>
     * Copying data is only required if it's been configured and it hasn't already completed.
     * </p>
     *
     * @return true if should copy the existing data.
     */
    private boolean shouldCopyData() {
        Map<String, Object> offset = getOffset(sourceConfig);
        return sourceConfig.getBoolean(COPY_EXISTING_CONFIG) && (offset == null || offset.containsKey("copy"));
    }

    /**
     * This method also is responsible for caching the {@code resumeAfter} value for the change stream.
     */
    private void setCachedResultAndResumeToken() {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> changeStreamCursor =
                getChangeStreamIterable(sourceConfig, mongoClient).cursor();
        ChangeStreamDocument<Document> firstResult = changeStreamCursor.tryNext();
        if (firstResult != null) {
            cachedResult = new BsonDocumentWrapper<>(firstResult, ChangeStreamDocument.createCodec(Document.class,
                    MongoClientSettings.getDefaultCodecRegistry()));
        }
        cachedResumeToken = firstResult != null ? firstResult.getResumeToken() : changeStreamCursor.getResumeToken();
        changeStreamCursor.close();
    }

    /**
     * Returns the next document to be delivered to Kafka.
     *
     * <p>
     * <ol>
     * <li>If copying data is in progress, returns the next result.</li>
     * <li>If copying data and all data has been copied and there is a cached result return the cached result.</li>
     * <li>Otherwise, return the next result from the change stream cursor. Creating a new cursor if necessary.</li>
     * </ol>
     * </p>
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
            isCopying.set(false);
            if (cachedResult != null) {
                result = Optional.of(cachedResult);
                cachedResult = null;
                return result;
            }
        }

        if (cursor == null) {
            cursor = createCursor(sourceConfig, mongoClient);
        }

        if (cursor != null) {
            try {
                BsonDocument next = cursor.tryNext();
                // The cursor has been closed by the server
                if (next == null && cursor.getServerCursor() == null) {
                    invalidatedCursor = true;
                    cursor.close();
                    cursor = null;
                    cursor = createCursor(sourceConfig, mongoClient);
                    next = cursor.tryNext();
                }
                return Optional.ofNullable(next);
            } catch (Exception e) {
                if (cursor != null) {
                    cursor.close();
                    cursor = null;
                }
                LOGGER.info("An exception occurred when trying to get the next item from the changestream.", e);
                return Optional.empty();
            }
        }
        return  Optional.empty();
    }

    private ChangeStreamIterable<Document> getChangeStreamIterable(final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {
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

    private Map<String, Object> getOffset(final MongoSourceConfig sourceConfig) {
        return context != null ? context.offsetStorageReader().offset(createPartitionMap(sourceConfig)) : null;
    }

    private BsonDocument getResumeToken(final MongoSourceConfig sourceConfig) {
        BsonDocument resumeToken = null;
        if (cachedResumeToken != null) {
            resumeToken = cachedResumeToken;
            cachedResumeToken = null;
        } else if (invalidatedCursor) {
            invalidatedCursor = false;
        } else {
            Map<String, Object> offset = getOffset(sourceConfig);
            if (offset != null && !offset.containsKey("initialSync")) {
                resumeToken = BsonDocument.parse((String) offset.get("_id"));
            }
        }
        return resumeToken;
    }
}
