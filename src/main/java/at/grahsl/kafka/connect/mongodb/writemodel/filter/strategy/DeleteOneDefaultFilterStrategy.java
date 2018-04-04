package at.grahsl.kafka.connect.mongodb.writemodel.filter.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

public class DeleteOneDefaultFilterStrategy implements WriteModelFilterStrategy {

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {

        BsonDocument kd = document.getKeyDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the key document was missing unexpectedly")
        );

        return  new DeleteOneModel<>(new BsonDocument(DBCollection.ID_FIELD_NAME, kd));

    }
}
