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
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

public class MongoDbDelete implements CdcOperation {

    @Override
    public WriteModel<BsonDocument> perform(SinkDocument doc) {

        BsonDocument keyDoc = doc.getKeyDoc().orElseThrow(
                () -> new DataException("error: key doc must not be missing for delete operation")
        );

        try {
            BsonDocument filterDoc = BsonDocument.parse(
                    "{"+DBCollection.ID_FIELD_NAME+
                        ":"+keyDoc.getString(MongoDbHandler.JSON_ID_FIELD_PATH)
                                .getValue()+"}"
            );
            return new DeleteOneModel<>(filterDoc);
        } catch(Exception exc) {
            throw new DataException(exc);
        }

    }

}
