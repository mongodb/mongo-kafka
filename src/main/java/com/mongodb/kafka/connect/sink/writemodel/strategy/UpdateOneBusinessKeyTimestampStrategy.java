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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ID_FIELD;
import static com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelHelper.flattenKeys;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.Configurable;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialKeyStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy;

public class UpdateOneBusinessKeyTimestampStrategy implements WriteModelStrategy, Configurable {

  private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);
  static final String FIELD_NAME_MODIFIED_TS = "_modifiedTS";
  static final String FIELD_NAME_INSERTED_TS = "_insertedTS";
  private boolean isPartialId = false;

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
    if (idValue == null || !idValue.isDocument()) {
      throw new DataException(
          "Could not build the WriteModel,the value document does not contain an _id field of"
              + " type BsonDocument which holds the business key fields.\n\n If you are including an"
              + " existing `_id` value in the business key then ensure `document.id.strategy.overwrite.existing=true`.");
    }

    BsonDateTime dateTime = new BsonDateTime(System.currentTimeMillis());
    BsonDocument businessKey = idValue.asDocument();
    vd.remove(ID_FIELD);
    if (isPartialId) {
      businessKey = flattenKeys(businessKey);
    }
    return new UpdateOneModel<>(
        businessKey,
        new BsonDocument("$set", vd.append(FIELD_NAME_MODIFIED_TS, dateTime))
            .append("$setOnInsert", new BsonDocument(FIELD_NAME_INSERTED_TS, dateTime)),
        UPDATE_OPTIONS);
  }

  @Override
  public void configure(final MongoSinkTopicConfig configuration) {
    IdStrategy idStrategy = configuration.getIdStrategy();
    isPartialId =
        idStrategy instanceof PartialKeyStrategy || idStrategy instanceof PartialValueStrategy;
  }
}
