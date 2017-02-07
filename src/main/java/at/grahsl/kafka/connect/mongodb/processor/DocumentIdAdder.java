package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import com.mongodb.DBCollection;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.types.ObjectId;

public class DocumentIdAdder extends PostProcessor {

    public DocumentIdAdder(MongoDbSinkConnectorConfig config) {
        super(config);
    }

    @Override
    public void process(BsonDocument doc, SinkRecord orig) {
        doc.append(DBCollection.ID_FIELD_NAME, new BsonObjectId(ObjectId.get()));
        next.ifPresent(pp -> pp.process(doc, orig));
    }

}
