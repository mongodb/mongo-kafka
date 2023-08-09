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

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class UpdateOneDefaultStrategy implements WriteModelStrategy {

  private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);

  @Override
  public WriteModel<BsonDocument> createWriteModel(final SinkDocument document) {
    BsonDocument vd =
        document
            .getValueDoc()
            .orElseThrow(
                () ->
                    new DataException(
                        "Could not build the WriteModel,the value document was missing unexpectedly"));

    BsonValue idValue = vd.get(ID_FIELD);
    if (idValue == null) {
      throw new DataException(
          "Could not build the WriteModel,the `_id` field was missing unexpectedly");
    }
    vd.remove(ID_FIELD);
    return new UpdateOneModel<>(
        new BsonDocument(ID_FIELD, idValue), new BsonDocument("$set", vd), UPDATE_OPTIONS);
  }
}
