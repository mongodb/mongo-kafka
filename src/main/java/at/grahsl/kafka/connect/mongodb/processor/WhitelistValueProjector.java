package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.field.projection.WhitelistProjector;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Set;
import java.util.function.Predicate;

public class WhitelistValueProjector extends WhitelistProjector {

    Predicate<MongoDbSinkConnectorConfig> predicate;

    public WhitelistValueProjector(MongoDbSinkConnectorConfig config) {
        this(config, config.getValueProjectionList(),
                cfg -> cfg.isUsingWhitelistValueProjection());
    }

    public WhitelistValueProjector(MongoDbSinkConnectorConfig config, Set<String> fields,
                                    Predicate<MongoDbSinkConnectorConfig> predicate) {
        super(config);
        this.fields = fields;
        this.predicate = predicate;
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {

        if(predicate.test(getConfig())) {
            doc.getValueDoc().ifPresent(vd ->
                    doProjection("", vd)
            );
        }

        getNext().ifPresent(pp -> pp.process(doc,orig));
    }

}
