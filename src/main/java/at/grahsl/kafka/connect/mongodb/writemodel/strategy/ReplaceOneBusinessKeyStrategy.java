package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class ReplaceOneBusinessKeyStrategy implements WriteModelStrategy {

    private static final UpdateOptions UPDATE_OPTIONS =
                                    new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        BsonValue businessKey = vd.get(DBCollection.ID_FIELD_NAME);

        if(businessKey == null || !(businessKey instanceof BsonDocument)) {
            throw new DataException("error: cannot build the WriteModel since"
                    + " the value document does not contain an _id field of type BsonDocument"
                    + " which holds the business key fields");
        }

        vd.remove(DBCollection.ID_FIELD_NAME);

        return new ReplaceOneModel<>((BsonDocument)businessKey, vd, UPDATE_OPTIONS);

    }
}
