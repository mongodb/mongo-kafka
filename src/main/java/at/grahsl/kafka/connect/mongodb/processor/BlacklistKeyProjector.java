package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.field.projection.BlacklistProjector;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Set;
import java.util.function.Predicate;

public class BlacklistKeyProjector extends BlacklistProjector {

    private Predicate<MongoDbSinkConnectorConfig> predicate;

    public BlacklistKeyProjector(MongoDbSinkConnectorConfig config) {
        this(config,config.getKeyProjectionList(),
                cfg -> cfg.isUsingBlacklistKeyProjection());
    }

    public BlacklistKeyProjector(MongoDbSinkConnectorConfig config, Set<String> fields,
                                 Predicate<MongoDbSinkConnectorConfig> predicate) {
        super(config);
        this.fields = fields;
        this.predicate = predicate;
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {

        if(predicate.test(getConfig())) {
            doc.getKeyDoc().ifPresent(kd ->
                    fields.forEach(f -> doProjection(f,kd))
            );
        }

        getNext().ifPresent(pp -> pp.process(doc,orig));
    }

}
