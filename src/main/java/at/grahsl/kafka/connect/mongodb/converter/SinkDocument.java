package at.grahsl.kafka.connect.mongodb.converter;

import org.bson.BsonDocument;

import java.util.Optional;

public class SinkDocument {

    private final Optional<BsonDocument> keyDoc;
    private final Optional<BsonDocument> valueDoc;

    public SinkDocument(BsonDocument keyDoc, BsonDocument valueDoc) {
        this.keyDoc = Optional.ofNullable(keyDoc);
        this.valueDoc = Optional.ofNullable(valueDoc);
    }

    public Optional<BsonDocument> getKeyDoc() {
        return keyDoc;
    }

    public Optional<BsonDocument> getValueDoc() {
        return valueDoc;
    }

}
