package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.field.projection.FieldProjector;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class PartialValueStrategy extends AbstractIdStrategy {

    FieldProjector fieldProjector;

    public PartialValueStrategy(FieldProjector fieldProjector) {
        super(MongoDbSinkConnectorConfig.IdStrategyModes.PARTIALVALUE);
        this.fieldProjector = fieldProjector;
    }

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {

        //NOTE: this has to operate on a clone because
        //otherwise it would interfere with further projections
        //happening later in the chain e.g. for value fields
        SinkDocument clone = doc.clone();
        fieldProjector.process(clone,orig);
        //NOTE: If there is no key doc present the strategy
        //simply returns an empty BSON document per default.
        return clone.getValueDoc().orElseGet(() -> new BsonDocument());

    }

}
