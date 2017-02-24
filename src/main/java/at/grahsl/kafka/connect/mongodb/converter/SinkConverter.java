package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SinkConverter {

    private static Logger logger = LoggerFactory.getLogger(SinkConverter.class);

    private RecordConverter avroConverter = new AvroRecordConverter();
    private RecordConverter jsonConverter = new JsonRecordConverter();

    public SinkDocument convert(SinkRecord record) {

        logger.debug(record.toString());

        BsonDocument keyDoc = null;
        if(record.key() != null && record.keySchema() != null
                && record.key() instanceof Struct) {
            keyDoc = avroConverter.convert(record.keySchema(), record.key());
        }

        if(record.key() != null && record.key() instanceof Map) {
            keyDoc = jsonConverter.convert(null, record.key());
        }

        BsonDocument valueDoc = null;
        if(record.value() != null && record.valueSchema() != null
                && record.value() instanceof Struct) {
            valueDoc = avroConverter.convert(record.valueSchema(), record.value());
        }

        if(record.value() != null && record.value() instanceof Map) {
            valueDoc = jsonConverter.convert(null, record.value());
        }

        return new SinkDocument(keyDoc, valueDoc);

    }

}
