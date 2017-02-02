package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonInt32;
import org.bson.BsonValue;

public class Int8FieldConverter extends SinkFieldConverter {

    public Int8FieldConverter() {
        super(Schema.INT8_SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonInt32(((Byte) data).intValue());
    }

}
