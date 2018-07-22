package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;

import java.time.Instant;

public class UpdateOneTimestampsStrategy implements WriteModelStrategy {

    public static final String FIELDNAME_MODIFIED_TS = "_modifiedTS";
    public static final String FIELDNAME_INSERTED_TS = "_insertedTS";

    private static final UpdateOptions UPDATE_OPTIONS =
                                    new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        BsonDateTime dateTime = new BsonDateTime(Instant.now().toEpochMilli());

        return new UpdateOneModel<>(
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        vd.get(DBCollection.ID_FIELD_NAME)),
                new BsonDocument("$set", vd.append(FIELDNAME_MODIFIED_TS, dateTime))
                        .append("$setOnInsert", new BsonDocument(FIELDNAME_INSERTED_TS, dateTime)),
                UPDATE_OPTIONS
        );

    }
}
