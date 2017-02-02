package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonValue;

public abstract class SinkFieldConverter extends FieldConverter {

    public SinkFieldConverter(Schema schema) {
        super(schema);
    }

    public abstract BsonValue toBson(Object data);

}
