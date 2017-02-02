package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDouble;
import org.bson.BsonValue;

public class Float64FieldConverter extends SinkFieldConverter {

    public Float64FieldConverter() {
        super(Schema.FLOAT64_SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonDouble((Double) data);
    }

}
