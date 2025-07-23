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

package com.mongodb.kafka.connect.sink.processor.id.strategy;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;

import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

abstract class BsonOidProvidedStrategy implements IdStrategy {

  protected enum ProvidedIn {
    KEY,
    VALUE
  }

  private ProvidedIn where;

  BsonOidProvidedStrategy(final ProvidedIn where) {
    this.where = where;
  }

  @Override
  public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
    Optional<BsonDocument> optionalDoc = Optional.empty();
    if (where.equals(ProvidedIn.KEY)) {
      optionalDoc = doc.getKeyDoc();
    }

    if (where.equals(ProvidedIn.VALUE)) {
      optionalDoc = doc.getValueDoc();
    }

    BsonValue id =
        optionalDoc
            .map(d -> d.get(ID_FIELD))
            .orElseThrow(
                () ->
                    new DataException(
                        "Provided id strategy is used but the document structure either contained"
                            + " no _id field or it was null"));

    if (id instanceof BsonNull) {
      throw new DataException(
          "Provided id strategy used but the document structure contained an _id of type BsonNull");
    }
	return new BsonObjectId(new ObjectId((((BsonString)id).getValue())));
  }
}

