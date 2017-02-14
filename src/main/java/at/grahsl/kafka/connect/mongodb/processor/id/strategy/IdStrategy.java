package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonValue;

public interface IdStrategy {

    BsonValue generateId(SinkDocument doc, SinkRecord orig);

}
