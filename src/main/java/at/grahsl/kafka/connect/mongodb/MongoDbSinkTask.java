/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb;

import at.grahsl.kafka.connect.mongodb.cdc.CdcHandler;
import at.grahsl.kafka.connect.mongodb.converter.SinkConverter;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.PostProcessor;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.DeleteOneDefaultStrategy;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.WriteModelStrategy;
import com.mongodb.BulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MongoDbSinkTask extends SinkTask {

    private static Logger LOGGER = LoggerFactory.getLogger(MongoDbSinkTask.class);

    private static final BulkWriteOptions BULK_WRITE_OPTIONS =
                            new BulkWriteOptions().ordered(false);

    private MongoDbSinkConnectorConfig sinkConfig;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private int remainingRetries;
    private int deferRetryMs;
    private PostProcessor processorChain;
    private CdcHandler cdcHandler;
    private WriteModelStrategy writeModelStrategy;
    private WriteModelStrategy deleteOneDefaultStrategy;
    private Map<String,MongoCollection<BsonDocument>> cachedCollections = new HashMap<>();

    private SinkConverter sinkConverter = new SinkConverter();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        LOGGER.info("starting MongoDB sink task");

        sinkConfig = new MongoDbSinkConnectorConfig(props);

        MongoClientURI uri = sinkConfig.buildClientURI();
        mongoClient = new MongoClient(uri);
        database = mongoClient.getDatabase(uri.getDatabase());

        remainingRetries = sinkConfig.getInt(
                MongoDbSinkConnectorConfig.MONGODB_MAX_NUM_RETRIES_CONF);
        deferRetryMs = sinkConfig.getInt(
                MongoDbSinkConnectorConfig.MONGODB_RETRIES_DEFER_TIMEOUT_CONF);

        processorChain = sinkConfig.buildPostProcessorChain();

        if(sinkConfig.isUsingCdcHandler()) {
            cdcHandler = sinkConfig.getCdcHandler();
        }

        writeModelStrategy = sinkConfig.getWriteModelStrategy();
        deleteOneDefaultStrategy = new DeleteOneDefaultStrategy();

    }

    @Override
    public void put(Collection<SinkRecord> records) {

        if(records.isEmpty()) {
            LOGGER.debug("no sink records to process for current poll operation");
            return;
        }

        Map<String, MongoDbSinkRecordBatches> batchMapping = createSinkRecordBatchesPerTopic(records);

        batchMapping.forEach((namespace, batches) ->
                batches.getBufferedBatches().forEach(batch ->
                        processSinkRecords(cachedCollections.get(namespace), batch)
                )
        );

    }

    private void processSinkRecords(MongoCollection<BsonDocument> collection, List<SinkRecord> batch) {
        List<? extends WriteModel<BsonDocument>> docsToWrite =
                sinkConfig.isUsingCdcHandler()
                        ? buildWriteModelCDC(batch)
                        : buildWriteModel(batch);
        try {
            if (!docsToWrite.isEmpty()) {
                LOGGER.debug("bulk writing {} document(s) into collection [{}]",
                        docsToWrite.size(), collection.getNamespace().getFullName());
                BulkWriteResult result = collection.bulkWrite(
                        docsToWrite, BULK_WRITE_OPTIONS);
                LOGGER.debug("mongodb bulk write result: " + result.toString());
            }
        } catch (MongoException mexc) {
            if (mexc instanceof BulkWriteException) {
                BulkWriteException bwe = (BulkWriteException) mexc;
                LOGGER.error("mongodb bulk write (partially) failed", bwe);
                LOGGER.error(bwe.getWriteResult().toString());
                LOGGER.error(bwe.getWriteErrors().toString());
                LOGGER.error(bwe.getWriteConcernError().toString());
            } else {
                LOGGER.error("error on mongodb operation", mexc);
                LOGGER.error("writing {} document(s) into collection [{}] failed -> remaining retries ({})",
                        docsToWrite.size(), collection.getNamespace().getFullName() ,remainingRetries);
            }
            if (remainingRetries-- <= 0) {
                throw new ConnectException("failed to write mongodb documents"
                        + " despite retrying -> GIVING UP! :( :( :(", mexc);
            }
            LOGGER.debug("deferring retry operation for {}ms", deferRetryMs);
            context.timeout(deferRetryMs);
            throw new RetriableException(mexc.getMessage(), mexc);
        }
    }

    private Map<String, MongoDbSinkRecordBatches> createSinkRecordBatchesPerTopic(Collection<SinkRecord> records) {
        LOGGER.debug("number of sink records to process: {}", records.size());

        Map<String,MongoDbSinkRecordBatches> batchMapping = new HashMap<>();
        int maxBatchSize = sinkConfig.getInt(MongoDbSinkConnectorConfig.MONGODB_MAX_BATCH_SIZE);
        LOGGER.debug("buffering sink records into grouped topic batches of at most size {}", maxBatchSize);
        records.forEach(r -> {

            String namespace = database.getName()+"."+sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF);
            MongoCollection<BsonDocument> mongoCollection = cachedCollections.get(namespace);
            if(mongoCollection == null) {
                mongoCollection = database.getCollection(
                        sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF), BsonDocument.class);
                cachedCollections.put(namespace,mongoCollection);
            }

            MongoDbSinkRecordBatches batches = batchMapping.get(namespace);

            if (batches == null) {
                batches = new MongoDbSinkRecordBatches(maxBatchSize,records.size());
                batchMapping.put(namespace,batches);
            }
            batches.buffer(r);
        });
        return batchMapping;
    }

    private List<? extends WriteModel<BsonDocument>>
                            buildWriteModel(Collection<SinkRecord> records) {

        List<WriteModel<BsonDocument>> docsToWrite = new ArrayList<>(records.size());
        LOGGER.debug("building write model for {} record(s)", records.size());
        records.forEach(record -> {
                    SinkDocument doc = sinkConverter.convert(record);
                    processorChain.process(doc, record);
                    if(doc.getValueDoc().isPresent()) {
                        docsToWrite.add(writeModelStrategy.createWriteModel(doc));
                    }
                    else {
                        if(doc.getKeyDoc().isPresent()
                                && sinkConfig.isDeleteOnNullValues()) {
                            docsToWrite.add(deleteOneDefaultStrategy.createWriteModel(doc));
                        }
                    }
                }
        );

        return docsToWrite;
    }

    private List<? extends WriteModel<BsonDocument>>
                            buildWriteModelCDC(Collection<SinkRecord> records) {
        LOGGER.debug("building CDC write model for {} record(s)", records.size());
        return records.stream()
                .map(sinkConverter::convert)
                .map(cdcHandler::handle)
                .flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty))
                .collect(Collectors.toList());

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        //NOTE: flush is not used for now...
    }

    @Override
    public void stop() {
        LOGGER.info("stopping MongoDB sink task");
        mongoClient.close();
    }

}
