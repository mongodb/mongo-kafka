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

package at.grahsl.kafka.connect.mongodb.processor.id.strategy;

import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.util.Optional;

import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_ID_FIELD;

public class ProvidedStrategy implements IdStrategy {

    protected enum ProvidedIn {
        KEY,
        VALUE
    }

    private ProvidedIn where;

    public ProvidedStrategy(final ProvidedIn where) {
        this.where = where;
    }

    @Override
    public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
        Optional<BsonDocument> bd = Optional.empty();
        if (where.equals(ProvidedIn.KEY)) {
            bd = doc.getKeyDoc();
        }

        if (where.equals(ProvidedIn.VALUE)) {
            bd = doc.getValueDoc();
        }

        BsonValue id = bd.map(d -> d.get(MONGODB_ID_FIELD))
                .orElseThrow(() -> new DataException("error: provided id strategy is used but the document structure either contained"
                        + " no _id field or it was null"));

        if (id instanceof BsonNull) {
            throw new DataException("error: provided id strategy used but the document structure contained an _id of type BsonNull");
        }
        return id;
    }
}
