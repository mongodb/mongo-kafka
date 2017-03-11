package at.grahsl.kafka.connect.mongodb.processor.field.projection;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import com.mongodb.DBCollection;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public abstract class BlacklistProjector extends FieldProjector {

    public BlacklistProjector(MongoDbSinkConnectorConfig config) {
        this(config,config.getValueProjectionList());
    }

    public BlacklistProjector(MongoDbSinkConnectorConfig config,
                              Set<String> fields) {
        super(config);
        this.fields = fields;
    }

    @Override
    protected void doProjection(String field, BsonDocument doc) {

        if(!field.contains(FieldProjector.SUB_FIELD_DOT_SEPARATOR)) {

            if(field.equals(FieldProjector.SINGLE_WILDCARD)
                    || field.equals(FieldProjector.DOUBLE_WILDCARD)) {
                handleWildcard(field,"",doc);
                return;
            }

            //NOTE: never try to remove the _id field
            if(!field.equals(DBCollection.ID_FIELD_NAME))
                doc.remove(field);

            return;
        }

        int dotIdx = field.indexOf(FieldProjector.SUB_FIELD_DOT_SEPARATOR);
        String firstPart = field.substring(0,dotIdx);
        String otherParts = field.length() >= dotIdx
                                ? field.substring(dotIdx+1) : "";

        if(firstPart.equals(FieldProjector.SINGLE_WILDCARD)
            || firstPart.equals(FieldProjector.DOUBLE_WILDCARD)) {
            handleWildcard(firstPart,otherParts,doc);
            return;
        }

        BsonValue value = doc.get(firstPart);
        if(value!=null && value.isDocument()) {
            doProjection(otherParts, (BsonDocument)value);
        }

    }

    private void handleWildcard(String firstPart, String otherParts, BsonDocument doc) {
        Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<String, BsonValue> entry = iter.next();
            BsonValue value = entry.getValue();

            //NOTE: never try to remove the _id field
            if(entry.getKey().equals(DBCollection.ID_FIELD_NAME))
                continue;

            if(firstPart.equals(FieldProjector.DOUBLE_WILDCARD)) {
                iter.remove();
            }

            if(firstPart.equals(FieldProjector.SINGLE_WILDCARD)) {
                if(!value.isDocument()) {
                    iter.remove();
                } else {
                    if(!otherParts.isEmpty()) {
                        doProjection(otherParts, (BsonDocument)value);
                    }
                }
            }
        }
    }

}
