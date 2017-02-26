package at.grahsl.kafka.connect.mongodb.converter.types.sink.bson;

import at.grahsl.kafka.connect.mongodb.converter.SinkFieldConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonBinary;
import org.bson.BsonValue;

import java.nio.ByteBuffer;

public class BytesFieldConverter extends SinkFieldConverter {

    public BytesFieldConverter() {
        super(Schema.BYTES_SCHEMA);
    }

    @Override
    public BsonValue toBson(Object data) {

        //obviously SinkRecords may contain different types
        //to represent byte arrays
        if(data instanceof ByteBuffer)
            return new BsonBinary(((ByteBuffer) data).array());

        if(data instanceof byte[])
            return new BsonBinary((byte[])data);

        throw new DataException("error: bytes field conversion failed to due "
                + "to unexpected object type "+ data.getClass().getName());

    }

}
