package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;

public class KafkaMetaAdder extends PostProcessor {

    public static final String KAFKA_META_DATA = "topic-partition-offset";

    public KafkaMetaAdder(MongoDbSinkConnectorConfig config) {
        super(config);
    }

    @Override
    public void process(BsonDocument doc, SinkRecord orig) {
        doc.put(KAFKA_META_DATA, new BsonString(orig.topic()
                + "-" + orig.kafkaPartition() + "-" + orig.kafkaOffset()));
        doc.put(orig.timestampType().name(), new BsonInt64(orig.timestamp()));
        next.ifPresent(pp -> pp.process(doc, orig));
    }

}
