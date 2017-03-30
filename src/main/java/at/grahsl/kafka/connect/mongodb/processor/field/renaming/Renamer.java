package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.PostProcessor;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class Renamer extends PostProcessor {

    //PATH PREFIXES used as a simple means to
    //distinguish whether we operate on key or value
    //structure of a record and match name mappings
    //or regexp patterns accordingly
    public static final String PATH_PREFIX_KEY = "key";
    public static final String PATH_PREFIX_VALUE = "value";

    public static final String SUB_FIELD_DOT_SEPARATOR = ".";

    public Renamer(MongoDbSinkConnectorConfig config) {
        super(config);
    }

    protected abstract String renamed(String path, String name);

    protected abstract boolean isActive();

    protected void doRenaming(String field, BsonDocument doc) {
        Map<String, BsonValue> temp = new LinkedHashMap<>();

        Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<String, BsonValue> entry = iter.next();
            String oldKey = entry.getKey();
            BsonValue value = entry.getValue();
            String newKey = renamed(field, oldKey);

            if(!oldKey.equals(newKey)) {
                //IF NEW KEY ALREADY EXISTS WE THEN DON'T RENAME
                //AS IT WOULD CAUSE OTHER DATA TO BE SILENTLY OVERWRITTEN
                //WHICH IS ALMOST NEVER WHAT YOU WANT
                //MAYBE LOG WARNING HERE?
                doc.computeIfAbsent(newKey, k -> temp.putIfAbsent(k,value));
                iter.remove();
            }

            if(value instanceof BsonDocument) {
                String pathToField = field+SUB_FIELD_DOT_SEPARATOR+newKey;
                doRenaming(pathToField, (BsonDocument)value);
            }
        }

        doc.putAll(temp);
    }

    @Override
    public void process(SinkDocument doc, SinkRecord orig) {

        if(isActive()) {
            doc.getKeyDoc().ifPresent(kd -> doRenaming(PATH_PREFIX_KEY, kd));
            doc.getValueDoc().ifPresent(vd -> doRenaming(PATH_PREFIX_VALUE, vd));
        }

    }

}
