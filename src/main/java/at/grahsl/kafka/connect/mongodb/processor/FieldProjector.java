package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;

import java.util.Set;

public abstract class FieldProjector extends PostProcessor {

    Set<String> fields;

    public FieldProjector(MongoDbSinkConnectorConfig config) {
        super(config);
    }

}
