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

package at.grahsl.kafka.connect.mongodb.cdc.debezium;

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

        BsonDocument filterDoc = doc.getKeyDoc().map(kd ->
                new BsonDocument(DBCollection.ID_FIELD_NAME,
                        kd.get(JSON_ID_FIELD_PATH)))
                    .orElseThrow(
                            () -> new DataException("error: creating filter doc for update failed")
                    );

        BsonDocument updateDoc = doc.getValueDoc().map(vd ->
                BsonDocument.parse(vd.get(JSON_DOC_FIELD_PATH).asString().getValue()))
                    .orElseThrow(
                            () -> new DataException("error: parsing update doc from JSON string failed")
                    );

        //NOTE: in this case the patch contains full new doc
        // e.g. when adding a new field to existing document
        // check potential difference when handling existing field, array, subdoc
        // check when deleting a field with $unset
        if(updateDoc.containsKey(DBCollection.ID_FIELD_NAME)) {
            return new ReplaceOneModel<>(
                    new BsonDocument(DBCollection.ID_FIELD_NAME,
                            updateDoc.get(DBCollection.ID_FIELD_NAME)),
                    updateDoc,
                    UPDATE_OPTIONS
            );
        }

        //NOTE: in this case the patch contains the idempotent change of doc
        return new UpdateOneModel<>(filterDoc, updateDoc);

    }

}
