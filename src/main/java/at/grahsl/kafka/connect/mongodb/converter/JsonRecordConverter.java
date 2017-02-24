package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Map;

public class JsonRecordConverter implements RecordConverter {

    private CodecRegistry codecRegistry =
                CodecRegistries.fromProviders(
                        new DocumentCodecProvider(),
                        new BsonValueCodecProvider(),
                        new ValueCodecProvider()
                );

    @Override
    public BsonDocument convert(Schema schema, Object value) {

        if(value == null) {
            throw new DataException("error: value was null for JSON conversion");
        }

        return new Document((Map)value).toBsonDocument(null, codecRegistry);

    }
}

