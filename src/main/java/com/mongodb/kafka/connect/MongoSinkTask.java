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

package com.mongodb.kafka.connect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.confluent.common.config.ConfigException;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.cdc.CdcHandler;
import com.mongodb.kafka.connect.converter.SinkConverter;
import com.mongodb.kafka.connect.converter.SinkDocument;
import com.mongodb.kafka.connect.processor.PostProcessor;
import com.mongodb.kafka.connect.writemodel.strategy.WriteModelStrategy;

public class MongoSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);
    private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions().ordered(false);  // TODO keep the order?

    private MongoSinkConnectorConfig sinkConfig;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private int remainingRetries;
    private int deferRetryMs;

    private Map<String, PostProcessor> processorChains;
    private Map<String, CdcHandler> cdcHandlers;
    private Map<String, WriteModelStrategy> writeModelStrategies;
    private Map<String, MongoSinkConnectorConfig.RateLimitSettings> rateLimitSettings;
    private Map<String, WriteModelStrategy> deleteOneModelDefaultStrategies;

    private SinkConverter sinkConverter = new SinkConverter();

    @Override
    public String version() {
        return Versions.VERSION;
    }

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override
    public void start(final Map<String, String> props) {
        LOGGER.info("Starting MongoDB sink task");
        try {
            sinkConfig = new MongoSinkConnectorConfig(props);
            ConnectionString connectionString = sinkConfig.getConnectionString();
            MongoDriverInformation driverInformation = MongoDriverInformation.builder()
                    .driverName(Versions.NAME).driverVersion(Versions.VERSION).build();
            mongoClient = MongoClients.create(connectionString, driverInformation);
            database = mongoClient.getDatabase(sinkConfig.getDatabaseName());

            remainingRetries = sinkConfig.getInt(MongoSinkConnectorConfig.MAX_NUM_RETRIES_CONFIG);
            deferRetryMs = sinkConfig.getInt(MongoSinkConnectorConfig.RETRIES_DEFER_TIMEOUT_CONFIG);

            processorChains = sinkConfig.buildPostProcessorChains();
            cdcHandlers = sinkConfig.getCdcHandlers();

            writeModelStrategies = sinkConfig.getWriteModelStrategies();
            rateLimitSettings = sinkConfig.getRateLimitSettings();
            deleteOneModelDefaultStrategies = sinkConfig.getDeleteOneModelDefaultStrategies();
        } catch (Exception e) {
            throw new ConfigException("Failed to start new task", e);
        }
        LOGGER.debug("Started MongoDB sink task");
    }

    /**
     * Put the records in the sink. Usually this should send the records to the sink asynchronously
     * and immediately return.
     *
     * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            LOGGER.debug("No sink records to process for current poll operation");
            return;
        }
        Map<String, MongoSinkRecordBatches> batchMapping = createSinkRecordBatchesPerTopic(records);
        batchMapping.forEach((collectionName, batches) -> {
            batches.getBufferedBatches().forEach(batch -> {
                        processSinkRecords(collectionName, batch);
                        MongoSinkConnectorConfig.RateLimitSettings rls =
                                rateLimitSettings.getOrDefault(collectionName,
                                        rateLimitSettings.get(MongoSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME));
                        if (rls.isTriggered()) {
                            LOGGER.debug("Rate limit settings triggering {}ms defer timeout after processing {}"
                                            + " further batches for collection {}", rls.getTimeoutMs(), rls.getEveryN(), collectionName);
                            try {
                                Thread.sleep(rls.getTimeoutMs());
                            } catch (InterruptedException e) {
                                LOGGER.error(e.getMessage());
                            }
                        }
                    }
            );
        });
    }

    /**
     * Flush all records that have been {@link #put(Collection)} for the specified topic-partitions.
     *
     * @param currentOffsets the current offset state as of the last call to {@link #put(Collection)}},
     *                       provided for convenience but could also be determined by tracking all offsets included in the
     *                       {@link SinkRecord}s passed to {@link #put}.
     */
    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        //NOTE: flush is not used for now...
        LOGGER.debug("Flush called - noop");
    }

    /**
     * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
     * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
     * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
     * as closing network connections to the sink system.
     */
    @Override
    public void stop() {
        LOGGER.info("Stopping MongoDB sink task");
        mongoClient.close();
    }

    private void processSinkRecords(final String collectionName, final List<SinkRecord> batch) {
        MongoCollection<BsonDocument> collection = database.getCollection(collectionName, BsonDocument.class);
        List<? extends WriteModel<BsonDocument>> writeModels = sinkConfig.isUsingCdcHandler(collectionName)
                ? buildWriteModelCDC(batch, collectionName) : buildWriteModel(batch, collectionName);
        try {
            if (!writeModels.isEmpty()) {
                LOGGER.debug("Bulk writing {} document(s) into collection [{}]", writeModels.size(),
                        collection.getNamespace().getFullName());
                BulkWriteResult result = collection.bulkWrite(writeModels, BULK_WRITE_OPTIONS);
                LOGGER.debug("Mongodb bulk write result: {}", result);
            }
        } catch (MongoBulkWriteException e) {
            LOGGER.error("Mongodb bulk write (partially) failed", e);
            LOGGER.error(e.getWriteResult().toString());
            LOGGER.error(e.getWriteErrors().toString());
            LOGGER.error(e.getWriteConcernError().toString());
            checkRetriableException(e);
        } catch (MongoException e) {
            LOGGER.error("Error on mongodb operation", e);
            LOGGER.error("Writing {} document(s) into collection [{}] failed -> remaining retries ({})",
                    writeModels.size(), collection.getNamespace().getFullName(), remainingRetries);
            checkRetriableException(e);
        }
    }

    private void checkRetriableException(final MongoException e) {
        if (remainingRetries-- <= 0) {
            throw new DataException("Failed to write mongodb documents despite retrying", e);
        }
        LOGGER.debug("Deferring retry operation for {}ms", deferRetryMs);
        context.timeout(deferRetryMs);
        throw new RetriableException(e.getMessage(), e);
    }

    Map<String, MongoSinkRecordBatches> createSinkRecordBatchesPerTopic(final Collection<SinkRecord> records) {
        LOGGER.debug("Number of sink records to process: {}", records.size());

        Map<String, MongoSinkRecordBatches> batchMapping = new HashMap<>();
        LOGGER.debug("Buffering sink records into grouped topic batches");
        records.forEach(r -> {
            String collection = sinkConfig.getString(MongoSinkConnectorConfig.COLLECTION_CONFIG, r.topic());
            if (collection.isEmpty()) {
                LOGGER.debug("No explicit collection name mapping found for topic {} and default collection name was empty.", r.topic());
                LOGGER.debug("Using topic name {} as collection name", r.topic());
                collection = r.topic();
            }
            MongoSinkRecordBatches batches = batchMapping.get(collection);

            if (batches == null) {
                int maxBatchSize = sinkConfig.getInt(MongoSinkConnectorConfig.MAX_BATCH_SIZE_CONFIG, collection);
                LOGGER.debug("Batch size for collection {} is at most {} record(s)", collection, maxBatchSize);
                batches = new MongoSinkRecordBatches(maxBatchSize, records.size());
                batchMapping.put(collection, batches);
            }
            batches.buffer(r);
        });
        return batchMapping;
    }

    List<? extends WriteModel<BsonDocument>> buildWriteModel(final Collection<SinkRecord> records, final String collectionName) {
        List<WriteModel<BsonDocument>> docsToWrite = new ArrayList<>(records.size());
        LOGGER.debug("building write model for {} record(s)", records.size());
        records.forEach(record -> {
            SinkDocument doc = sinkConverter.convert(record);
            processorChains.getOrDefault(collectionName, processorChains.get(MongoSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME))
                    .process(doc, record);
            if (doc.getValueDoc().isPresent()) {
                        docsToWrite.add(writeModelStrategies.getOrDefault(collectionName,
                                writeModelStrategies.get(MongoSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME)).createWriteModel(doc));
                    } else {
                        if (doc.getKeyDoc().isPresent() && sinkConfig.isDeleteOnNullValues(record.topic())) {
                            docsToWrite.add(deleteOneModelDefaultStrategies.getOrDefault(collectionName,
                                    deleteOneModelDefaultStrategies.get(MongoSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME))
                                    .createWriteModel(doc));
                        } else {
                            LOGGER.error("skipping sink record {} for which neither key doc nor value doc were present", record);
                        }
                    }
                }
        );

        return docsToWrite;
    }

    List<? extends WriteModel<BsonDocument>>
    buildWriteModelCDC(final Collection<SinkRecord> records, final String collectionName) {
        LOGGER.debug("Building CDC write model for {} record(s) into collection {}", records.size(), collectionName);
        return records.stream()
                .map(sinkConverter::convert)
                .map(cdcHandlers.get(collectionName)::handle)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());

    }
}
