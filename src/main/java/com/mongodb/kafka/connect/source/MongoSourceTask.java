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
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PUBLISH_FULL_DOCUMENT_ONLY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.getMongoDriverInformation;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

import java.util.ArrayList;
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
import org.bson.BsonString;
import org.bson.Document;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import com.mongodb.kafka.connect.Versions;


public class MongoSourceTask extends SourceTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSourceTask.class);
    private static final String INVALIDATE = "invalidate";

    private final Time time;
    private final AtomicBoolean isRunning = new AtomicBoolean();
    private MongoSourceConfig sourceConfig;
    private MongoClient mongoClient;
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
        cursor = createCursor(sourceConfig, mongoClient);
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
                Map<String, String> sourceOffset = singletonMap("_id", changeStreamDocument.getDocument("_id").toJson());
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
                    LOGGER.trace("Adding {} to {}", json, topicName);
                    String keyJson = new BsonDocument("_id", changeStreamDocument.get("_id")).toJson();
                    sourceRecords.add(new SourceRecord(partition, sourceOffset, topicName, Schema.STRING_SCHEMA, keyJson,
                            Schema.STRING_SCHEMA, json));
                });

                // If the cursor is invalidated add the record and return calls
                if (changeStreamDocument.getString("operationType", new BsonString("")).getValue().equalsIgnoreCase(INVALIDATE)) {
                    LOGGER.info("Cursor has been invalidated.");
                    cursor = null;
                    return sourceRecords;
                } else if (sourceRecords.size() == maxBatchSize) {
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
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }

    MongoCursor<BsonDocument> createCursor(final MongoSourceConfig cfg, final MongoClient mongoClient) {
        LOGGER.debug("Creating a MongoCursor");
        ChangeStreamIterable<Document> changeStream = getChangeStreamIterable(cfg, mongoClient);

        Map<String, Object> offset = context != null ? context.offsetStorageReader().offset(createPartitionMap(cfg)) : null;
        if (offset != null) {
            LOGGER.info("Resuming the change stream at the previous offset");
            changeStream.resumeAfter(BsonDocument.parse((String) offset.get("_id")));
        }
        LOGGER.debug("Cursor created");
        return changeStream.withDocumentClass(BsonDocument.class).iterator();
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

    Map<String, Object> createPartitionMap(final MongoSourceConfig cfg) {
        return singletonMap("ns", format("%s/%s.%s", cfg.getString(CONNECTION_URI_CONFIG),
                cfg.getString(DATABASE_CONFIG), cfg.getString(COLLECTION_CONFIG)));
    }

    /**
     * Returns the next document to be delivered to Kafka.
     *
     * <p>
     * Return the next result from the change stream cursor. Creating the cursor if necessary.
     * </p>
     *
     * @return the next document
     */
    private Optional<BsonDocument> getNextDocument() {
        if (cursor == null) {
            cursor = createCursor(sourceConfig, mongoClient);
        }

        try {
            return Optional.ofNullable(cursor.tryNext());
        } catch (Exception e) {
            cursor = null;
            return Optional.empty();
        }
    }

    private ChangeStreamIterable<Document> getChangeStreamIterable(final MongoSourceConfig cfg,
                                                                   final MongoClient mongoClient) {
        String database = cfg.getString(DATABASE_CONFIG);
        String collection = cfg.getString(COLLECTION_CONFIG);

        Optional<List<Document>> pipeline = cfg.getPipeline();
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

        int batchSize = cfg.getInt(BATCH_SIZE_CONFIG);
        if (batchSize > 0) {
            changeStream.batchSize(batchSize);
        }
        cfg.getFullDocument().ifPresent(changeStream::fullDocument);
        cfg.getCollation().ifPresent(changeStream::collation);
        return changeStream;
    }
}
