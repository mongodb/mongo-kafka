package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Timestamp;
import org.bson.BsonDateTime;
import org.bson.BsonValue;

public class TimestampFieldConverter extends SinkFieldConverter {

    public TimestampFieldConverter() {
        super(Timestamp.SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonDateTime(((java.util.Date)data).getTime());
    }
}
