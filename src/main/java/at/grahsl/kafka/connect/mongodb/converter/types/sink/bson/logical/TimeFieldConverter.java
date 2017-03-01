package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Time;
import org.bson.BsonDateTime;
import org.bson.BsonValue;

public class TimeFieldConverter extends SinkFieldConverter {

    public TimeFieldConverter() {
        super(Time.SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonDateTime(((java.util.Date)data).getTime());
    }
}
