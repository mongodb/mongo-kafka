package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonString;
import org.bson.BsonValue;

public class StringFieldConverter extends SinkFieldConverter {

    public StringFieldConverter() {
        super(Schema.STRING_SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonString((String) data);
    }

}
