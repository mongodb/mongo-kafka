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
import com.mongodb.DBCollection;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;

import java.time.Instant;

public class UpdateOneTimestampsStrategy implements WriteModelStrategy {
    private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);
    static final String FIELD_NAME_MODIFIED_TS = "_modifiedTS";
    static final String FIELD_NAME_INSERTED_TS = "_insertedTS";

    @Override
    public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {

        BsonDocument vd = document.getValueDoc().orElseThrow(
                () -> new DataException("error: cannot build the WriteModel since"
                        + " the value document was missing unexpectedly")
        );

        BsonDateTime dateTime = new BsonDateTime(Instant.now().toEpochMilli());

        return new UpdateOneModel<>(
                new BsonDocument(DBCollection.ID_FIELD_NAME, vd.get(DBCollection.ID_FIELD_NAME)),
                new BsonDocument("$set", vd.append(FIELD_NAME_MODIFIED_TS, dateTime))
                        .append("$setOnInsert", new BsonDocument(FIELD_NAME_INSERTED_TS, dateTime)),
                UPDATE_OPTIONS
        );

    }
}
