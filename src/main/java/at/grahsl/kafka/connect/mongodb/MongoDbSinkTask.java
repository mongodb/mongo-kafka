package at.grahsl.kafka.connect.mongodb;

import at.grahsl.kafka.connect.mongodb.converter.SinkConverter;
import com.mongodb.BulkWriteException;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertOneModel;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.types.ObjectId;
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

    private SinkConverter sinkConverter = new SinkConverter();

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        sinkConfig = new MongoDbSinkConnectorConfig(map);
        mongoClient = new MongoClient(sinkConfig.buildClientURI());
        database = mongoClient.getDatabase(
                sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_DATABASE_CONF));
    }

    @Override
    public void put(Collection<SinkRecord> kafkaCollection) {

        MongoCollection<BsonDocument> mongoCollection = database.getCollection(
                sinkConfig.getString(MongoDbSinkConnectorConfig.MONGODB_COLLECTION_CONF),
                            BsonDocument.class);

        List<InsertOneModel<BsonDocument>> docsToWrite = new ArrayList<>();

        kafkaCollection.forEach(sr ->
                sinkConverter.convert(sr).getValueDoc().ifPresent(
                        bd -> {
                            BsonObjectId oid = new BsonObjectId(ObjectId.get());
                            bd.append(DBCollection.ID_FIELD_NAME, oid);
                            docsToWrite.add(new InsertOneModel<>(bd));
                        }
                )
        );

        try {
            if (!docsToWrite.isEmpty()) {
                logger.debug("records to write: {}",docsToWrite.size());
                BulkWriteResult result = mongoCollection.bulkWrite(docsToWrite);
                logger.debug(result.toString());
            } else {
                logger.debug("nothing to write for current poll operation");
            }
        } catch(BulkWriteException exc) {
            logger.error("mongodb bulk write failed");
            logger.error(exc.getWriteResult().toString());
            logger.error(exc.getWriteErrors().toString());
        } catch(MongoException exc) {
            logger.error("error on mongodb operation",exc);
            throw new RetriableException(exc.getMessage(),exc);
        }

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void stop() {
       mongoClient.close();
    }

}
