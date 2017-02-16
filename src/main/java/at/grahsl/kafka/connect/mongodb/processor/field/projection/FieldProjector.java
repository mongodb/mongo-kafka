package at.grahsl.kafka.connect.mongodb.processor.field.projection;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.processor.PostProcessor;
import org.bson.BsonDocument;

import java.util.Set;

public abstract class FieldProjector extends PostProcessor {

    public static final String SINGLE_WILDCARD = "*";
    public static final String DOUBLE_WILDCARD = "**";
    public static final String SUB_FIELD_DOT_SEPARATOR = ".";

    protected Set<String> fields;

    public FieldProjector(MongoDbSinkConnectorConfig config) {
        super(config);
    }

    protected abstract void doProjection(String field, BsonDocument doc);

}
