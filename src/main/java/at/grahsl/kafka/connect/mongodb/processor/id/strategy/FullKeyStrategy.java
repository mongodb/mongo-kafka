package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class FullKeyStrategy extends AbstractIdStrategy {

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {
        //NOTE: If there is no key doc present the strategy
        //simply returns an empty BSON document per default.
        return doc.getKeyDoc().orElseGet(() -> new BsonDocument());
    }

}
