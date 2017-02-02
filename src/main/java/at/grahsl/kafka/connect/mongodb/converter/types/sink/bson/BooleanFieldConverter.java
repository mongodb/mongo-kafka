package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonBoolean;
import org.bson.BsonValue;

public class BooleanFieldConverter extends SinkFieldConverter {

    public BooleanFieldConverter() {
        super(Schema.BOOLEAN_SCHEMA);
    }

    public BsonValue toBson(Object data) {
        return new BsonBoolean((Boolean) data);
    }

}
