package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import com.mongodb.DBCollection;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class WhitelistProjector extends FieldProjector {

    public WhitelistProjector(MongoDbSinkConnectorConfig config,
                              Set<String> fields) {
        super(config);
        this.fields = fields;
    }

    @Override
    public void process(BsonDocument doc, SinkRecord orig) {

        Iterator<Map.Entry<String,BsonValue>> iter = doc.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<String,BsonValue> entry = iter.next();
            //NOTE: always keep the _id field if present
            if(!fields.contains(entry.getKey())
                    && !entry.getKey().equals(DBCollection.ID_FIELD_NAME)) {
                iter.remove();
            }
        }

        next.ifPresent(pp -> pp.process(doc,orig));
    }
}
