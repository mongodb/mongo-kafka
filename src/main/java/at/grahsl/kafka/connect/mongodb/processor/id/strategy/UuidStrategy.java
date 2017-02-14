package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.UUID;

public class UuidStrategy extends AbstractIdStrategy {

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {
        return new BsonString(UUID.randomUUID().toString());
    }

}
