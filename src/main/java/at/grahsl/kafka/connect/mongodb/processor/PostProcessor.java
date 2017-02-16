package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Optional;

public abstract class PostProcessor {

    final MongoDbSinkConnectorConfig config;
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

    public abstract void process(SinkDocument doc, SinkRecord orig);

    public MongoDbSinkConnectorConfig getConfig() {
        return this.config;
    }

    public Optional<PostProcessor> getNext() {
        return this.next;
    }

}
