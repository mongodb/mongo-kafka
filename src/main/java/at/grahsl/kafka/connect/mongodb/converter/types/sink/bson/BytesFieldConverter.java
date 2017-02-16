package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonBinary;
import org.bson.BsonValue;

import java.nio.ByteBuffer;

public class BytesFieldConverter extends SinkFieldConverter {

    public BytesFieldConverter() {
        super(Schema.BYTES_SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {
        return new BsonBinary(((ByteBuffer) data).array());
    }

}
