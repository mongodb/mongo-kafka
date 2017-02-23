package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;

public abstract class AbstractIdStrategy implements IdStrategy {

    MongoDbSinkConnectorConfig.IdStrategyModes mode;

    public AbstractIdStrategy(MongoDbSinkConnectorConfig.IdStrategyModes mode) {
        this.mode = mode;
    }

    public MongoDbSinkConnectorConfig.IdStrategyModes getMode() {
        return mode;
    }

}
