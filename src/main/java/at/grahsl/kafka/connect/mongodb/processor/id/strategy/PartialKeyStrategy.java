package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.field.projection.FieldProjector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class PartialKeyStrategy extends AbstractIdStrategy {

    FieldProjector fieldProjector;

    public PartialKeyStrategy(FieldProjector fieldProjector) {
        super(MongoDbSinkConnectorConfig.IdStrategyModes.PARTIALKEY);
        this.fieldProjector = fieldProjector;
    }

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {

        fieldProjector.process(doc,orig);
        //NOTE: If there is no key doc present the strategy
        //simply returns an empty BSON document per default.
        return doc.getKeyDoc().orElseGet(() -> new BsonDocument());

    }

}
