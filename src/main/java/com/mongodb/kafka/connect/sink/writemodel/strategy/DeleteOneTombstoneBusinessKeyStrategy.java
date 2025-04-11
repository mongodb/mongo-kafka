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
 */

package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelHelper.flattenKeys;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.Configurable;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialKeyStrategy;

public class DeleteOneTombstoneBusinessKeyStrategy implements WriteModelStrategy, Configurable {
  private IdStrategy idStrategy;

  @Override
  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
    document
        .getKeyDoc()
        .orElseThrow(
            () ->
                new DataException(
                    "Could not build the WriteModel,the key document was missing unexpectedly"));

    if (!(idStrategy instanceof PartialKeyStrategy)) {
      throw new ConnectException(
          "DeleteOneTombstoneBusinessKeyStrategy expects PartialKeyStrategy to be defined");
    }

    BsonDocument businessKey = idStrategy.generateId(document, null).asDocument();
    businessKey = flattenKeys(businessKey);
    return new DeleteOneModel<>(businessKey);
  }

  @Override
  public void configure(final MongoSinkTopicConfig configuration) {
    idStrategy = configuration.getIdStrategy();
  }
}
