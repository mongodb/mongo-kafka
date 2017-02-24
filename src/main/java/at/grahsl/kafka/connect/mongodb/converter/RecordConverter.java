package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDocument;

public interface RecordConverter {

    BsonDocument convert(Schema schema, Object value);

}
