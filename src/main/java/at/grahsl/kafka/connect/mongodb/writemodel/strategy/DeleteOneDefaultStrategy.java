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

package at.grahsl.kafka.connect.mongodb.writemodel.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.id.strategy.IdStrategy;
import com.mongodb.DBCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public class DeleteOneDefaultStrategy implements WriteModelStrategy {

    private IdStrategy idStrategy;

    @Deprecated
    public DeleteOneDefaultStrategy() {
    }

    public DeleteOneDefaultStrategy(IdStrategy idStrategy) {
        this.idStrategy = idStrategy;
    }

    @Override
    public WriteModel<BsonDocument> createWriteModel(SinkDocument document) {

        BsonDocument kd = document.getKeyDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the key document was missing unexpectedly")
        );

        //NOTE: fallback for backwards / deprecation compatibility
        if (idStrategy == null) {
            return kd.containsKey(DBCollection.ID_FIELD_NAME)
                    ? new DeleteOneModel<>(kd)
                    : new DeleteOneModel<>(new BsonDocument(DBCollection.ID_FIELD_NAME, kd));
        }

        //NOTE: current design doesn't allow to access original SinkRecord (= null)
        BsonValue _id = idStrategy.generateId(document, null);
        return new DeleteOneModel<>(
                new BsonDocument(DBCollection.ID_FIELD_NAME, _id)
        );

    }
}
