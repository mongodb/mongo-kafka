package at.grahsl.kafka.connect.mongodb.processor;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import com.mongodb.DBCollection;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class WhitelistProjector extends FieldProjector {

    public WhitelistProjector(MongoDbSinkConnectorConfig config) {
        this(config, config.getFieldProjectionList());
    }

    public WhitelistProjector(MongoDbSinkConnectorConfig config,
                              Set<String> fields) {
        super(config);
        this.fields = fields;
    }

    @Override
    public void process(BsonDocument doc, SinkRecord orig) {

        if(config.isUsingWhitelistProjection())
            doProjection("",doc);

        next.ifPresent(pp -> pp.process(doc,orig));
    }

    @Override
    void doProjection(String field, BsonDocument doc) {

        Iterator<Map.Entry<String, BsonValue>> iter = doc.entrySet().iterator();
        while(iter.hasNext()) {
            Map.Entry<String, BsonValue> entry = iter.next();

            String key = field.isEmpty() ? entry.getKey()
                    : field + FieldProjector.SUB_FIELD_DOT_SEPARATOR + entry.getKey();
            BsonValue value = entry.getValue();

            if(!fields.contains(key)
                    //NOTE: always keep the _id field
                    && !key.equals(DBCollection.ID_FIELD_NAME)) {

                //check if single wildcard match
                //not exists for currrent sub field
                if(!field.isEmpty()) {
                    String singleWildcardMatch = field
                            + FieldProjector.SUB_FIELD_DOT_SEPARATOR
                            + FieldProjector.SINGLE_WILDCARD;
                    if(!fields.contains(singleWildcardMatch)) {
                        iter.remove();
                    }
                } else {
                    iter.remove();
                    continue;
                }
            }

            if(value.isDocument()) {
                //check if double wildcard match
                //not exists for current field
                //and only then recurse
                String matchDoubleWildCard = key
                        + FieldProjector.SUB_FIELD_DOT_SEPARATOR
                        + FieldProjector.DOUBLE_WILDCARD;
                if(!fields.contains(matchDoubleWildCard)) {
                    doProjection(key, (BsonDocument)value);
                }
            }

        }
    }
}
