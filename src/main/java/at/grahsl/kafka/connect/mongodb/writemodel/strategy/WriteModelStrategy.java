package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;

public interface WriteModelStrategy {

    WriteModel<BsonDocument> createWriteModel(SinkDocument document);

}
