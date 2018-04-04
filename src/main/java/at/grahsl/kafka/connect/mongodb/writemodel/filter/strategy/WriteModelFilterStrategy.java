package at.grahsl.kafka.connect.mongodb.writemodel.filter.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.client.model.WriteModel;
import org.bson.BsonDocument;

public interface WriteModelFilterStrategy {

    WriteModel<BsonDocument> createWriteModel(SinkDocument document);

}
