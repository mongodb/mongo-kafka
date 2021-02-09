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

package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy;

public class DeleteOneDefaultStrategy implements WriteModelStrategy {
  private IdStrategy idStrategy;

  public DeleteOneDefaultStrategy() {
    this(new DefaultIdFieldStrategy());
  }

  public DeleteOneDefaultStrategy(final IdStrategy idStrategy) {
    this.idStrategy = idStrategy;
  }

  @Override
  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {

    document
        .getKeyDoc()
        .orElseThrow(
            () ->
                new DataException(
                    "Could not build the WriteModel,the key document was missing unexpectedly"));

    // NOTE: current design doesn't allow to access original SinkRecord (= null)
    BsonDocument deleteFilter;
    if (idStrategy instanceof DefaultIdFieldStrategy) {
      deleteFilter = idStrategy.generateId(document, null).asDocument();
    } else {
      deleteFilter = new BsonDocument(ID_FIELD, idStrategy.generateId(document, null));
    }
    return new DeleteOneModel<>(deleteFilter);
  }

  static class DefaultIdFieldStrategy implements IdStrategy {
    @Override
    public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
      BsonDocument kd = doc.getKeyDoc().get();
      return kd.containsKey(ID_FIELD) ? kd : new BsonDocument(ID_FIELD, kd);
    }
  }
}
