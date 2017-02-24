package at.grahsl.kafka.connect.mongodb.converter;

import at.grahsl.kafka.connect.mongodb.converter.types.sink.bson.*;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonArray;
import org.bson.BsonDocument;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroRecordConverter implements RecordConverter {

    private final Map<Schema.Type, SinkFieldConverter> knownConverters = new HashMap<>();

    public AvroRecordConverter() {
        registerSinkFieldConverter(new BooleanFieldConverter());
        registerSinkFieldConverter(new Int8FieldConverter());
        registerSinkFieldConverter(new Int16FieldConverter());
        registerSinkFieldConverter(new Int32FieldConverter());
        registerSinkFieldConverter(new Int64FieldConverter());
        registerSinkFieldConverter(new Float32FieldConverter());
        registerSinkFieldConverter(new Float64FieldConverter());
        registerSinkFieldConverter(new StringFieldConverter());
        registerSinkFieldConverter(new BytesFieldConverter());
    }

    @Override
    public BsonDocument convert(Schema schema, Object value) {

        if(schema == null || value == null) {
            throw new DataException("error: schema and/or value was null for AVRO conversion");
        }

        return toBsonDoc(schema, value);

    }

    private void registerSinkFieldConverter(SinkFieldConverter converter) {
        knownConverters.put(converter.getSchema().type(), converter);
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
                case BYTES:
                    doc.put(field.name(), getConverter(field.schema()).toBson(struct.get(field),field.schema()));
                    break;
                case STRUCT:
                    doc.put(field.name(), toBsonDoc(field.schema(), struct.get(field)));
                    break;
                case ARRAY:
                    BsonArray array = new BsonArray();
                    for(Object element : (List)struct.get(field)) {
                        if(field.schema().valueSchema().type().isPrimitive()) {
                            array.add(getConverter(field.schema().valueSchema()).toBson(element,field.schema()));
                        } else {
                            array.add(toBsonDoc(field.schema().valueSchema(), element));
                        }
                    }
                    doc.put(field.name(), array);
                    break;
                case MAP:
                    BsonDocument bd = new BsonDocument();
                    Map m = (Map)struct.get(field);
                    for(Object entry : m.keySet()) {
                        String key = (String)entry;
                        if(field.schema().valueSchema().type().isPrimitive()) {
                            bd.put(key, getConverter(field.schema().valueSchema()).toBson(m.get(key),field.schema()));
                        } else {
                            bd.put(key, toBsonDoc(field.schema().valueSchema(), m.get(key)));
                        }
                    }
                    doc.put(field.name(), bd);
                    break;
                default:
                    throw new DataException("unexpected / unsupported schema type " + field.schema().type());
            }
        } catch (Exception exc) {
            throw new DataException("error while processing field " + field.name(), exc);
        }

    }

    private SinkFieldConverter getConverter(Schema schema) {

        SinkFieldConverter converter = knownConverters.get(schema.type());

        if (converter == null) {
            throw new ConnectException("error no registered converter found for " + schema.type().getName());
        }

        return converter;
    }
}
