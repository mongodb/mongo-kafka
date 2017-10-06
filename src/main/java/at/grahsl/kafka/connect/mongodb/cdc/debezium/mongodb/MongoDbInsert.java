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
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

public class MongoDbInsert implements CdcOperation {

    public static final String JSON_DOC_FIELD_PATH = "after";

    private static final UpdateOptions UPDATE_OPTIONS =
            new UpdateOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for insert operation")
        );

        try {
            BsonDocument insertDoc = BsonDocument.parse(
                    valueDoc.get(JSON_DOC_FIELD_PATH).asString().getValue()
            );
            return new ReplaceOneModel<>(
                    new BsonDocument(DBCollection.ID_FIELD_NAME,
                            insertDoc.get(DBCollection.ID_FIELD_NAME)),
                    insertDoc,
                    UPDATE_OPTIONS
            );
        } catch(Exception exc) {
            throw new DataException(exc);
        }

    }

}
