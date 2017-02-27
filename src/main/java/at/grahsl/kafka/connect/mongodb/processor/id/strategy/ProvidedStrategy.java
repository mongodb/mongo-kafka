package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.util.Optional;

public class ProvidedStrategy extends AbstractIdStrategy {

    public ProvidedStrategy(MongoDbSinkConnectorConfig.IdStrategyModes mode) {
        super(mode);
    }

    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {

        Optional<BsonDocument> bd = Optional.empty();

        if(mode.equals(MongoDbSinkConnectorConfig.IdStrategyModes.PROVIDEDINKEY)) {
            bd = doc.getKeyDoc();
        }

        if(mode.equals(MongoDbSinkConnectorConfig.IdStrategyModes.PROVIDEDINVALUE)) {
            bd = doc.getValueDoc();
        }

        BsonValue _id = bd.map(vd -> vd.get(DBCollection.ID_FIELD_NAME))
                    .orElseThrow(() -> new DataException("error: provided id strategy is used "
                        + "but the document structure either contained no _id field or it was null"));

        if(_id instanceof BsonNull) {
            throw new DataException("error: provided id strategy used "
                    + "but the document structure contained an _id of type BsonNull");
        }

        return _id;

    }

}
