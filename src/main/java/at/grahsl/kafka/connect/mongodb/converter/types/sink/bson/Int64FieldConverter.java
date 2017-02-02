package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonInt64;
import org.bson.BsonValue;

public class Int64FieldConverter extends SinkFieldConverter {

    public Int64FieldConverter() {
        super(Schema.INT64_SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonInt64((Long) data);
    }

}
