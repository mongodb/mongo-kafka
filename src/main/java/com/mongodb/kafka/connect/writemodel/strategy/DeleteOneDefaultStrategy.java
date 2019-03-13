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

import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.converter.SinkDocument;
import com.mongodb.kafka.connect.processor.id.strategy.IdStrategy;

public class DeleteOneDefaultStrategy implements WriteModelStrategy {

    private IdStrategy idStrategy;

    @Deprecated
    public DeleteOneDefaultStrategy() {
    }

    public DeleteOneDefaultStrategy(final IdStrategy idStrategy) {
        this.idStrategy = idStrategy;
    }

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {

        BsonDocument kd = document.getKeyDoc().orElseThrow(
                () -> new DataException("Error: cannot build the WriteModel since"
                        + " the key document was missing unexpectedly")
        );

        //NOTE: fallback for backwards / deprecation compatibility
        if (idStrategy == null) {
            return kd.containsKey(ID_FIELD) ? new DeleteOneModel<>(kd)
                    : new DeleteOneModel<>(new BsonDocument(ID_FIELD, kd));
        }

        //NOTE: current design doesn't allow to access original SinkRecord (= null)
        return new DeleteOneModel<>(new BsonDocument(ID_FIELD, idStrategy.generateId(document, null)));
    }
}
