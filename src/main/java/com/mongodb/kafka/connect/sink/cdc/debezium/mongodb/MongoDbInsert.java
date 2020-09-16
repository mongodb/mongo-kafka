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

package com.mongodb.kafka.connect.sink.cdc.debezium.mongodb;

import static com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler.ID_FIELD;
import static java.lang.String.format;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class MongoDbInsert implements CdcOperation {

  private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);
  private static final String JSON_DOC_FIELD_PATH = "after";

  @Override
  public WriteModel<BsonDocument> perform(final SinkDocument doc) {

    BsonDocument valueDoc =
        doc.getValueDoc()
            .orElseThrow(
                () -> new DataException("Value document must not be missing for insert operation"));

    if (!valueDoc.containsKey(JSON_DOC_FIELD_PATH)) {
      throw new DataException(format("Insert document missing `%s` field.", JSON_DOC_FIELD_PATH));
    }

    try {
      BsonDocument insertDoc =
          BsonDocument.parse(valueDoc.get(JSON_DOC_FIELD_PATH).asString().getValue());
      return new ReplaceOneModel<>(
          new BsonDocument(ID_FIELD, insertDoc.get(ID_FIELD)), insertDoc, REPLACE_OPTIONS);
    } catch (Exception exc) {
      throw new DataException(exc);
    }
  }
}
