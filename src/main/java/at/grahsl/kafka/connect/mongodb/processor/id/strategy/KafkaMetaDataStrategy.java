package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonString;
import org.bson.BsonValue;

public class KafkaMetaDataStrategy extends AbstractIdStrategy {

    public static final String DELIMITER = "#";

    public KafkaMetaDataStrategy() {
        super(MongoDbSinkConnectorConfig.IdStrategyModes.KAFKAMETA);
    }

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {

       return new BsonString(orig.topic()
                        + DELIMITER + orig.kafkaPartition()
                        + DELIMITER + orig.kafkaOffset());

    }

}
