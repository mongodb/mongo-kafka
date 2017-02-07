package at.grahsl.kafka.connect.mongodb;

import at.grahsl.kafka.connect.mongodb.converter.SinkConverter;
import at.grahsl.kafka.connect.mongodb.processor.*;
import com.mongodb.BulkWriteException;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
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

public class MongoDbSinkTask extends SinkTask {

    private static Logger logger = LoggerFactory.getLogger(MongoDbSinkTask.class);

    private MongoDbSinkConnectorConfig sinkConfig;
    private MongoClient mongoClient;
    private MongoDatabase database;
    private int remainingRetries;
    private int deferRetryMs;
    private PostProcessor processorChain;

    private SinkConverter sinkConverter = new SinkConverter();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        logger.info("starting MongoDB sink task");

        sinkConfig = new MongoDbSinkConnectorConfig(props);
        mongoClient = new MongoClient(sinkConfig.buildClientURI());

        database = mongoClient.getDatabase(
                sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_DATABASE_CONF));

        remainingRetries = sinkConfig.getInt(
                MongoDbSinkConnectorConfig.MONGODB_MAX_NUM_RETRIES_CONF);
        deferRetryMs = sinkConfig.getInt(
                MongoDbSinkConnectorConfig.MONGODB_RETRIES_DEFER_TIMEOUT_CONF);

        processorChain = new DocumentIdAdder(sinkConfig);

        if(sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(MongoDbSinkConnectorConfig.FieldProjectionTypes.BLACKLIST.name())) {
            processorChain.chain(new BlacklistProjector(sinkConfig, sinkConfig.getFieldProjectionList()));
        }

        if(sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(MongoDbSinkConnectorConfig.FieldProjectionTypes.WHITELIST.name())) {
            processorChain.chain(new WhitelistProjector(sinkConfig, sinkConfig.getFieldProjectionList()));
        }
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

        List<InsertOneModel<BsonDocument>> docsToWrite = new ArrayList<>();

        records.forEach(record ->
                sinkConverter.convert(record).getValueDoc().ifPresent(
                        doc -> {
                            processorChain.process(doc,record);
                            docsToWrite.add(new InsertOneModel<>(doc));
                        }
                )
        );

        try {
            logger.debug("#records to write: {}", docsToWrite.size());
            BulkWriteResult result = mongoCollection.bulkWrite(docsToWrite,
                    new BulkWriteOptions().ordered(false));
            logger.debug("write result: "+result.toString());
        } catch(BulkWriteException exc) {
            logger.error("mongodb bulk write (partially) failed");
            logger.error(exc.getWriteResult().toString());
            logger.error(exc.getWriteErrors().toString());
            logger.error(exc.getWriteConcernError().toString());
        } catch(MongoException exc) {
            logger.error("error on mongodb operation",exc);
            logger.error("writing {} record(s) failed - remaining retries ({})",
                    records.size(),remainingRetries);
            if(remainingRetries-- == 0) {
                throw new ConnectException("couldn't successfully process records despite retrying",exc);
            }
            logger.debug("deferring retry operation for {}ms",deferRetryMs);
            context.timeout(deferRetryMs);
            throw new RetriableException(exc.getMessage(),exc);
        }

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void stop() {
        logger.info("stopping MongoDB sink task");
        mongoClient.close();
    }

}
