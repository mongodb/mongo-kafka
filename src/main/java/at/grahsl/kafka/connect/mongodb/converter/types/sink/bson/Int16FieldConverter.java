package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonInt32;
import org.bson.BsonValue;

public class Int16FieldConverter extends SinkFieldConverter {

    public Int16FieldConverter() {
        super(Schema.INT16_SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonInt32(((Short) data).intValue());
    }

}
