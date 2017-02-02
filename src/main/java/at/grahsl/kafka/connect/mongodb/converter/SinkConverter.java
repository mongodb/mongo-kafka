package at.grahsl.kafka.connect.mongodb.converter;

import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SinkConverter {

    private final Map<Schema.Type, SinkFieldConverter> knownConverters = new HashMap<>();

    private static Logger logger = LoggerFactory.getLogger(SinkConverter.class);

    public SinkConverter() {
        registerSinkFieldConverter(new BooleanFieldConverter());
        registerSinkFieldConverter(new Int8FieldConverter());
        registerSinkFieldConverter(new Int16FieldConverter());
        registerSinkFieldConverter(new Int32FieldConverter());
        registerSinkFieldConverter(new Int64FieldConverter());
        registerSinkFieldConverter(new Float32FieldConverter());
        registerSinkFieldConverter(new Float64FieldConverter());
        registerSinkFieldConverter(new StringFieldConverter());
    }

    private void registerSinkFieldConverter(SinkFieldConverter converter) {
        knownConverters.put(converter.getSchema().type(), converter);
    }

    public SinkDocument convert(SinkRecord record) {

        logger.info(record.toString());

        BsonDocument keyDoc = null;
        if (record.key() != null) {
            keyDoc = toBsonDoc(record.keySchema(), record.value());
        }

        BsonDocument valueDoc = null;
        if (record.value() != null) {
            valueDoc = toBsonDoc(record.valueSchema(), record.value());
        }

        return new SinkDocument(keyDoc, valueDoc);

    }

    private BsonDocument toBsonDoc(Schema schema, Object value) {
        BsonDocument doc = new BsonDocument();
        schema.fields().forEach(f -> processField(doc, (Struct)value, f));
        return doc;
    }

    private void processField(BsonDocument doc, Struct struct, Field field) {

        try {
            switch(field.schema().type()) {
                case BOOLEAN:
                case FLOAT32:
                case FLOAT64:
                case INT8:
                case INT16:
                case INT32:
                case INT64:
                case STRING:
                    doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field)));
                    break;
                case STRUCT:
                    doc.put(field.name(), toBsonDoc(field.schema(), struct.get(field)));
                    break;
                case ARRAY:
                    BsonArray array = new BsonArray();
                    for(Object element : (List)struct.get(field)) {
                        if(field.schema().valueSchema().type().isPrimitive()) {
                            array.add(getConverter(field.schema().valueSchema()).toBson(element));
                        } else {
                            array.add(toBsonDoc(field.schema().valueSchema(), element));
                        }
                    }
                    doc.put(field.name(), array);
                    break;
                case MAP:
                case BYTES:
                default:
                    throw new DataException("unexpected / unsupported schema type " + field.schema().type());
            }
        } catch (Exception exc) {
            throw new DataException("error while processing field " + field.name(), exc);
        }

    }

    public SinkFieldConverter getConverter(Schema schema) {

        SinkFieldConverter converter = knownConverters.get(schema.type());

        if (converter == null) {
            throw new ConnectException("error no registered converter found for " + schema.type().getName());
        }

        return converter;
    }

}
