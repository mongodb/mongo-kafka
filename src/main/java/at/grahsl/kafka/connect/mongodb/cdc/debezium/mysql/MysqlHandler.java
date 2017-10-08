/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.cdc.debezium.mysql;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.cdc.CdcOperation;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.DebeziumCdcHandler;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.OperationType;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class MysqlHandler extends DebeziumCdcHandler {

    public static final String JSON_DOC_BEFORE_FIELD = "before";
    public static final String JSON_DOC_AFTER_FIELD = "after";

    private static Logger logger = LoggerFactory.getLogger(MysqlHandler.class);

    public MysqlHandler(MongoDbSinkConnectorConfig config) {
        super(config);
        final Map<OperationType,CdcOperation> operations = new HashMap<>();
        operations.put(OperationType.CREATE,new MysqlInsert());
        operations.put(OperationType.READ,new MysqlInsert());
        operations.put(OperationType.UPDATE,new MysqlUpdate());
        operations.put(OperationType.DELETE,new MysqlDelete());
        registerOperations(operations);
    }

    @Override
    public WriteModel<BsonDocument> handle(SinkDocument doc) {
        BsonDocument keyDoc = doc.getKeyDoc().orElseGet(() -> new BsonDocument());
        logger.debug("key: "+keyDoc.toJson());
        BsonDocument valueDoc = doc.getValueDoc().orElseGet(() -> new BsonDocument());
        logger.debug("value: "+valueDoc.toJson());
        if ( (keyDoc.size() >= 0 && valueDoc.isEmpty()) ) {
            logger.debug("skipping debezium tombstone event for kafka topic compaction");
            return null;
        }
        return getCdcOperation(valueDoc).perform(new SinkDocument(keyDoc,valueDoc));
    }

    public static BsonDocument generateFilterDoc(BsonDocument keyDoc, BsonDocument valueDoc,OperationType opType) {
        BsonDocument filterDoc = new BsonDocument();
        String[] fields = keyDoc.keySet().toArray(new String[0]);
        if (fields.length == 0) {
            if(opType.equals(OperationType.CREATE) || opType.equals(OperationType.READ)) {
                //no PK info in keyDoc -> generate ObjectId
                filterDoc.put(DBCollection.ID_FIELD_NAME,new BsonObjectId());
            } else {
                //no PK info in keyDoc -> take everything in 'before' field
                filterDoc = valueDoc.getDocument(JSON_DOC_BEFORE_FIELD);
            }
        } else {
            //build filter document composed of all PK columns
            BsonDocument pk = new BsonDocument();
            for (String f : fields) {
                pk.put(f,keyDoc.get(f));
            }
            filterDoc.put(DBCollection.ID_FIELD_NAME,pk);
        }
        return filterDoc;
    }

    public static BsonDocument generateUpsertOrReplaceDoc(BsonDocument keyDoc, BsonDocument valueDoc, BsonDocument filterDoc) {
        BsonDocument upsertDoc = new BsonDocument(
                DBCollection.ID_FIELD_NAME,filterDoc.get(DBCollection.ID_FIELD_NAME)
        );
        if (!valueDoc.containsKey(JSON_DOC_AFTER_FIELD)
                || valueDoc.get(JSON_DOC_AFTER_FIELD).isNull()
                || !valueDoc.get(JSON_DOC_AFTER_FIELD).isDocument()) {
            throw new DataException("error: valueDoc must contain 'after' field" +
                    " of type document for insert/update operation");
        }
        BsonDocument afterDoc = valueDoc.getDocument(JSON_DOC_AFTER_FIELD);
        for(String f : afterDoc.keySet()) {
            if(!keyDoc.containsKey(f)) {
                upsertDoc.put(f,afterDoc.get(f));
            }
        }
        return upsertDoc;
    }

}
