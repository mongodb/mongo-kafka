package com.mongodb.kafka.connect.sink.processor.id.strategy;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.*;

import java.util.Optional;
import java.util.UUID;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;

public class UuidInValueStrategy implements IdStrategy {
    @Override
    public BsonValue generateId(SinkDocument doc, SinkRecord orig) {
        Optional<BsonDocument> optionalDoc = doc.getValueDoc();

        BsonValue id = optionalDoc.map(d -> d.get(ID_FIELD))
                .orElseThrow(() -> new DataException("Error: provided id strategy is used but the document structure either contained"
                        + " no _id field or it was null"));

        if (id instanceof BsonNull) {
            throw new DataException("Error: provided id strategy used but the document structure contained an _id of type BsonNull");
        }

        return new BsonBinary(UUID.fromString(id.asString().getValue()), UuidRepresentation.STANDARD);
    }
}
