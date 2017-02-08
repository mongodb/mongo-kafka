package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;

import java.util.Optional;

public abstract class PostProcessor {

    MongoDbSinkConnectorConfig config;
    Optional<PostProcessor> next = Optional.empty();

    public PostProcessor(MongoDbSinkConnectorConfig config) {
        this.config = config;
    }

    public PostProcessor chain(PostProcessor next) {
        // intentionally throws NPE here if someone
        // tries to be 'smart' by chaining with null
        this.next = Optional.of(next);
        return this.next.get();
    }

    public abstract void process(BsonDocument doc, SinkRecord orig);

}
