package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.id.strategy.IdStrategy;
import com.mongodb.DBCollection;
import org.apache.kafka.connect.sink.SinkRecord;

public class DocumentIdAdder extends PostProcessor {

    IdStrategy idStrategy;

    public DocumentIdAdder(MongoDbSinkConnectorConfig config) {
        this(config,config.getIdStrategy());
    }

    public DocumentIdAdder(MongoDbSinkConnectorConfig config, IdStrategy idStrategy) {
        super(config);
        this.idStrategy = idStrategy;
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {
        doc.getValueDoc().ifPresent(vd ->
            vd.append(DBCollection.ID_FIELD_NAME, idStrategy.generateId(doc,orig))
        );
        next.ifPresent(pp -> pp.process(doc, orig));
    }

}
