package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonNull;
import org.bson.BsonValue;

public abstract class SinkFieldConverter extends FieldConverter {

    public SinkFieldConverter(Schema schema) {
        super(schema);
    }

    public abstract BsonValue toBson(Object data);

    public BsonValue toBson(Object data, Schema fieldSchema) {
        if(!fieldSchema.isOptional()) {

            if(data == null)
                throw new DataException("error: schema not optional but data was null");

            return toBson(data);
        }

        if(data != null) {
            return toBson(data);
        }

        if(fieldSchema.defaultValue() != null) {
            return toBson(fieldSchema.defaultValue());
        }

        return new BsonNull();
    }

}
