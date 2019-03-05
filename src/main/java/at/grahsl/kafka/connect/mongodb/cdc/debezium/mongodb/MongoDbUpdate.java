/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2017 Hans-Peter Grahsl.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

    public static final String JSON_DOC_FIELD_PATH = "patch";

    private static final UpdateOptions UPDATE_OPTIONS =
            new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> perform(final SinkDocument doc) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for update operation")
        );

        try {

            BsonDocument updateDoc = BsonDocument.parse(
                    valueDoc.getString(JSON_DOC_FIELD_PATH).getValue()
            );

            //patch contains full new document for replacement
            if (updateDoc.containsKey(DBCollection.ID_FIELD_NAME)) {
                BsonDocument filterDoc =
                        new BsonDocument(DBCollection.ID_FIELD_NAME,
                                updateDoc.get(DBCollection.ID_FIELD_NAME));
                return new ReplaceOneModel<>(filterDoc, updateDoc, UPDATE_OPTIONS);
            }

            //patch contains idempotent change only to update original document with
            BsonDocument keyDoc = doc.getKeyDoc().orElseThrow(
                    () -> new DataException("error: key doc must not be missing for update operation")
            );

            BsonDocument filterDoc = BsonDocument.parse(
                    "{" + DBCollection.ID_FIELD_NAME +
                            ":" + keyDoc.getString(MongoDbHandler.JSON_ID_FIELD_PATH)
                            .getValue() + "}"
            );

            return new UpdateOneModel<>(filterDoc, updateDoc);

        } catch (DataException exc) {
            exc.printStackTrace();
            throw exc;
        } catch (Exception exc) {
            exc.printStackTrace();
            throw new DataException(exc.getMessage(), exc);
        }

    }

}
