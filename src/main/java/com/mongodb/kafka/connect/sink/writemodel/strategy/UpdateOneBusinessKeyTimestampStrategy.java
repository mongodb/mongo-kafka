package com.mongodb.kafka.connect.sink.writemodel.strategy;

import com.mongodb.client.model.*;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BSONException;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;

import java.time.Instant;

/**
 * Use when the client does not produce the _id but does have a unique
 * business key that will identify a unique document especially for updates.
 *
 * Config settings for the Sink Connector:
 *  document.id.strategy=com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy
 *  value.projection.type=whitelist
 *  value.projection.list=<businessKey field(s)>
 *
 * Will also add  _modifiedTS and _insertedTS timestamps as default.
 *
 * Essentially the same as the ReplaceOneBusinessKeyStrategy except it will
 * updateOne() thus providing delta values to downstream clients who might perhaps
 * be using Change Streams as an example.
 *
 *
 */
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;

public class UpdateOneBusinessKeyTimestampStrategy implements WriteModelStrategy {

    private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);
    static final String FIELD_NAME_MODIFIED_TS = "_modifiedTS";
    static final String FIELD_NAME_INSERTED_TS = "_insertedTS";

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("Error: cannot build the WriteModel since " +
                        "the value document was missing unexpectedly"));

        BsonDateTime dateTime = new BsonDateTime(Instant.now().toEpochMilli());

        try {
            BsonDocument businessKey = vd.getDocument(ID_FIELD);
            vd.remove(ID_FIELD);

            return new UpdateOneModel<>(
                    businessKey,
                    new BsonDocument("$set", vd.append(FIELD_NAME_MODIFIED_TS, dateTime))
                            .append("$setOnInsert", new BsonDocument(FIELD_NAME_INSERTED_TS, dateTime)),
                    UPDATE_OPTIONS);

        } catch (BSONException e) {
            throw new DataException("Error: cannot build the WriteModel since the value " +
                    "document does not contain an _id field of"
                    + " type BsonDocument which holds the business key fields");
        }
    }

}
