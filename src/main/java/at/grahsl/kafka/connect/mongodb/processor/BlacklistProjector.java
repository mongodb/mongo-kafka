package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import com.mongodb.DBCollection;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;

import java.util.Set;

public class BlacklistProjector extends FieldProjector {

    public BlacklistProjector(MongoDbSinkConnectorConfig config,
                              Set<String> fields) {
        super(config);
        this.fields = fields;
    }

    @Override
    public void process(BsonDocument doc, SinkRecord orig) {
        //NOTE: never try to remove _id field
        fields.forEach(f -> {
            if (!f.equalsIgnoreCase(DBCollection.ID_FIELD_NAME))
                doc.remove(f);
        });
        next.ifPresent(pp -> pp.process(doc,orig));
    }
}
