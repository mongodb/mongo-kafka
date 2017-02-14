package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonInt64;
import org.bson.BsonString;

public class KafkaMetaAdder extends PostProcessor {

    public static final String KAFKA_META_DATA = "topic-partition-offset";

    public KafkaMetaAdder(MongoDbSinkConnectorConfig config) {
        super(config);
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {

        doc.getValueDoc().ifPresent(vd -> {
            vd.put(KAFKA_META_DATA, new BsonString(orig.topic()
                    + "-" + orig.kafkaPartition() + "-" + orig.kafkaOffset()));
            vd.put(orig.timestampType().name(), new BsonInt64(orig.timestamp()));
        });

        next.ifPresent(pp -> pp.process(doc, orig));
    }

}
