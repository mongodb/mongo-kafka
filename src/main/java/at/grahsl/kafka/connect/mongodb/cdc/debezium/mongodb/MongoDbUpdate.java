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

package at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb;

import at.grahsl.kafka.connect.mongodb.cdc.CdcOperation;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import com.mongodb.DBCollection;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

public class MongoDbUpdate implements CdcOperation {

    public static final String JSON_ID_FIELD_PATH = "_id";
    public static final String JSON_DOC_FIELD_PATH = "patch";

    private static final UpdateOptions UPDATE_OPTIONS =
            new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc) {

        BsonDocument updateDoc = doc.getValueDoc()
                .map(vd ->
                    BsonDocument.parse(vd.get(JSON_DOC_FIELD_PATH).asString().getValue()))
                .orElseThrow(
                    () -> new DataException("error: parsing update doc from JSON string failed")
                );

        //NOTE: in the one case the patch contains full new doc to replace old one
        if(updateDoc.containsKey(DBCollection.ID_FIELD_NAME)) {
            //HERE WE CAN TAKE THE _id FIELD FROM THE PATCH
            //FIELD OF THE VALUE STRUCT OF THE SINK RECORD
            //WHICH SEEMS TO ALWAYS EXHIBIT THE CORRECT TYPE
            BsonDocument filterDoc =
                    new BsonDocument(DBCollection.ID_FIELD_NAME,
                        updateDoc.get(DBCollection.ID_FIELD_NAME));
            return new ReplaceOneModel<>(filterDoc, updateDoc, UPDATE_OPTIONS);
        }

        //NOTE: in the other case the patch only contains the idempotent change
        BsonDocument filterDoc = doc.getKeyDoc()
                .map(kd ->
                    new BsonDocument(DBCollection.ID_FIELD_NAME,
                        kd.get(JSON_ID_FIELD_PATH)))
                .orElseThrow(
                        () -> new DataException("error: creating filter doc for update failed")
                );

        //CURRENTLY THE DEBEZIUM MONGODB CDC SOURCE CONNECTOR
        //HAS A POTENTIAL ISSUE BY NOT SPECIFYING THE TYPE OF
        //THE _id FIELD IN THE KEY STRUCTURE CORRECTLY.
        //AS LONG AS THIS ISN'T FIXED WE CAN ONLY SUPPORT
        //TO HANDLE _id FIELDS OF TYPE STRING WHEN DEALING
        //WITH THE IDEMPOTENT UPDATE CHANGE EVENTS
        //SINCE THE _id FIELD ISN'T PART OF THE VALUE STRUCT.

        //NUMERIC ARE STRINGS
        //_id: "1234" instead of _id: 1234

        //OBJECTID ONLY GIVEN AS THE HEX STRING
        //while it should probably be like this
        // "_id" : {"$oid" : "595e5ad67713992e94ec99d7"}
        //it's instead represented like this
        // "_id" : "595e5ad67713992e94ec99d7"

        //DOCUMENTS ARE JSON SERIALIZED BUT QUOTE-ESCAPED JAVA STRINGS

        return new UpdateOneModel<>(filterDoc, updateDoc);

    }

}
