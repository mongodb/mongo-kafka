package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import org.bson.BsonDocument;

import java.util.Set;

public abstract class FieldProjector extends PostProcessor {

    public static final String SINGLE_WILDCARD = "*";
    public static final String DOUBLE_WILDCARD = "**";
    public static final String SUB_FIELD_DOT_SEPARATOR = ".";

    Set<String> fields;

    public FieldProjector(MongoDbSinkConnectorConfig config) {
        super(config);
    }

    abstract void doProjection(String field, BsonDocument doc);

}
