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
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;

import static at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb.MongoDbHandler.ID_FIELD;

public class MongoDbInsert implements CdcOperation {

    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);
    private static final String JSON_DOC_FIELD_PATH = "after";

    @Override
    public WriteModel<BsonDocument> perform(final SinkDocument doc) {

        BsonDocument valueDoc = doc.getValueDoc().orElseThrow(
                () -> new DataException("error: value doc must not be missing for insert operation")
        );

        try {
            BsonDocument insertDoc = BsonDocument.parse(valueDoc.get(JSON_DOC_FIELD_PATH).asString().getValue());
            return new ReplaceOneModel<>(new BsonDocument(ID_FIELD, insertDoc.get(ID_FIELD)), insertDoc, REPLACE_OPTIONS);
        } catch (Exception exc) {
            throw new DataException(exc);
        }

    }

}
