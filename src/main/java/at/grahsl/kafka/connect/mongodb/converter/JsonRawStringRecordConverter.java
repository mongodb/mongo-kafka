package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

public class JsonRawStringRecordConverter implements RecordConverter {

    @Override
    public BsonDocument convert(Schema schema, Object value) {

        if(value == null) {
            throw new DataException("error: value was null for JSON conversion");
        }

        return BsonDocument.parse((String)value);

    }
}

