package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;

public abstract class AbstractIdStrategy implements IdStrategy {

    MongoDbSinkConnectorConfig config;

    public AbstractIdStrategy() {}

    public AbstractIdStrategy(MongoDbSinkConnectorConfig config) {
        this.config = config;
    }

}
