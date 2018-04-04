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
import at.grahsl.kafka.connect.mongodb.writemodel.filter.strategy.DeleteOneDefaultFilterStrategy;
import at.grahsl.kafka.connect.mongodb.writemodel.filter.strategy.WriteModelFilterStrategy;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MongoDbSinkTask extends SinkTask {

    private static Logger logger = LoggerFactory.getLogger(MongoDbSinkTask.class);

    private static final BulkWriteOptions BULK_WRITE_OPTIONS =
                            new BulkWriteOptions().ordered(false);

    private MongoDbSinkConnectorConfig sinkConfig;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private int remainingRetries;
    private int deferRetryMs;
    private PostProcessor processorChain;
    private CdcHandler cdcHandler;
    private WriteModelFilterStrategy replaceOneFilterStrategy;
    private WriteModelFilterStrategy deleteOneFilterStrategy;

    private SinkConverter sinkConverter = new SinkConverter();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("starting MongoDB sink task");

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

        replaceOneFilterStrategy = sinkConfig.getReplaceOneFilterStrategy();
        deleteOneFilterStrategy = new DeleteOneDefaultFilterStrategy();

    }

    @Override
    public void put(Collection<SinkRecord> records) {

        if(records.isEmpty()) {
            logger.debug("no records to write for current poll operation");
            return;
        }

        MongoCollection<BsonDocument> mongoCollection = database.getCollection(
                sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF),
                            BsonDocument.class);

        List<? extends WriteModel<BsonDocument>> docsToWrite =
                        sinkConfig.isUsingCdcHandler()
                            ? buildWriteModelCDC(records)
                                : buildWriteModel(records);

        try {
            logger.debug("#records to write: {}", docsToWrite.size());
            if(!docsToWrite.isEmpty()) {
                BulkWriteResult result = mongoCollection.bulkWrite(
                        docsToWrite,BULK_WRITE_OPTIONS);
                logger.debug("write result: "+result.toString());
            }
        } catch(MongoException mexc) {

            if(mexc instanceof BulkWriteException) {
                BulkWriteException bwe = (BulkWriteException)mexc;
                logger.error("mongodb bulk write (partially) failed", bwe);
                logger.error(bwe.getWriteResult().toString());
                logger.error(bwe.getWriteErrors().toString());
                logger.error(bwe.getWriteConcernError().toString());
            } else {
                logger.error("error on mongodb operation" ,mexc);
                logger.error("writing {} record(s) failed -> remaining retries ({})",
                        records.size(),remainingRetries);
            }

            if(remainingRetries-- <= 0) {
                throw new ConnectException("couldn't successfully process records"
                        + " despite retrying -> giving up :(",mexc);
            }

            logger.debug("deferring retry operation for {}ms",deferRetryMs);
            context.timeout(deferRetryMs);
            throw new RetriableException(mexc.getMessage(),mexc);
        }

    }

    private List<? extends WriteModel<BsonDocument>>
                            buildWriteModel(Collection<SinkRecord> records) {

        List<WriteModel<BsonDocument>> docsToWrite = new ArrayList<>(records.size());

        records.forEach(record -> {
                    SinkDocument doc = sinkConverter.convert(record);
                    processorChain.process(doc, record);
                    if(doc.getValueDoc().isPresent()) {
                        docsToWrite.add(replaceOneFilterStrategy.createWriteModel(doc));
                    }
                    else {
                        if(doc.getKeyDoc().isPresent()
                                && sinkConfig.isDeleteOnNullValues()) {
                            docsToWrite.add(deleteOneFilterStrategy.createWriteModel(doc));
                        }
                    }
                }
        );

        return docsToWrite;
    }

    private List<? extends WriteModel<BsonDocument>>
                            buildWriteModelCDC(Collection<SinkRecord> records) {

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
        logger.info("stopping MongoDB sink task");
        mongoClient.close();
    }

}
