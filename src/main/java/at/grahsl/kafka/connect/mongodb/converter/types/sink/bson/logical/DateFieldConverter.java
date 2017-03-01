package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.logical;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Date;
import org.bson.BsonDateTime;
import org.bson.BsonValue;

public class DateFieldConverter extends SinkFieldConverter {

    public DateFieldConverter() {
        super(Date.SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonDateTime(((java.util.Date)data).getTime());
    }
}
