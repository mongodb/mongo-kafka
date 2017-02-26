package at.grahsl.kafka.connect.mongodb.converter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SinkConverter {

    private static Logger logger = LoggerFactory.getLogger(SinkConverter.class);

    private RecordConverter schemafulConverter = new AvroJsonSchemafulRecordConverter();
    private RecordConverter schemalessConverter = new JsonSchemalessRecordConverter();
    private RecordConverter rawConverter = new JsonRawStringRecordConverter();

    public SinkDocument convert(SinkRecord record) {

        logger.debug(record.toString());

        BsonDocument keyDoc = null;
        if(record.key() != null) {
            keyDoc = getRecordConverter(record.key(),record.keySchema())
                            .convert(record.keySchema(), record.key());
        }

        BsonDocument valueDoc = null;
        if(record.value() != null) {
            valueDoc = getRecordConverter(record.value(),record.valueSchema())
                    .convert(record.valueSchema(), record.value());
        }

        return new SinkDocument(keyDoc, valueDoc);

    }

    private RecordConverter getRecordConverter(Object data, Schema schema) {

        if(data == null) {
            throw new DataException("error: data must not be null when determining record converter");
        }

        //AVRO or JSON with schema
        if(schema != null && data instanceof Struct) {
            logger.debug("using schemaful converter");
            return schemafulConverter;
        }

        //structured JSON without schema
        if(data instanceof Map) {
            logger.debug("using schemaless converter");
            return schemalessConverter;
        }

        //raw JSON string
        if(data instanceof String) {
            logger.debug("using raw converter");
            return rawConverter;
        }

        throw new DataException("error: no converter present due to unexpected object type "
                                    + data.getClass().getName());
    }

}
