package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

public class BsonOidStrategy extends AbstractIdStrategy {

    public BsonOidStrategy() {
        super(MongoDbSinkConnectorConfig.IdStrategyModes.OBJECTID);
    }

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {
        return new BsonObjectId(ObjectId.get());
    }

}
