/*
 * Copyright 2008-present MongoDB, Inc.
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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.writemodel.strategy;

import static com.mongodb.kafka.connect.MongoSinkConnectorConfig.ID_FIELD;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BSONException;
import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.converter.SinkDocument;

public class ReplaceOneBusinessKeyStrategy implements WriteModelStrategy {

    private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("Error: cannot build the WriteModel since the value document was missing unexpectedly"));

        try {
            BsonDocument businessKey = vd.getDocument(ID_FIELD);
            vd.remove(ID_FIELD);
            return new ReplaceOneModel<>(businessKey, vd, REPLACE_OPTIONS);
        } catch (BSONException e) {
            throw new DataException("Error: cannot build the WriteModel since the value document does not contain an _id field of"
                    + " type BsonDocument which holds the business key fields");
        }
    }
}
