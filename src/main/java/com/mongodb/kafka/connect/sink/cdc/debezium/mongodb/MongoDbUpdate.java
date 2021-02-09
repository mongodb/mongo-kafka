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
import static com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler.JSON_ID_FIELD;
import static java.lang.String.format;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class MongoDbUpdate implements CdcOperation {
  private static final ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);
  private static final String JSON_DOC_FIELD_PATH = "patch";
  public static final String INTERNAL_OPLOG_FIELD_V = "$v";

  @Override
  public WriteModel<BsonDocument> perform(final SinkDocument doc) {

    BsonDocument valueDoc =
        doc.getValueDoc()
            .orElseThrow(
                () -> new DataException("Value document must not be missing for update operation"));

    if (!valueDoc.containsKey(JSON_DOC_FIELD_PATH)) {
      throw new DataException(format("Update document missing `%s` field.", JSON_DOC_FIELD_PATH));
    }

    try {
      BsonDocument updateDoc =
          BsonDocument.parse(valueDoc.getString(JSON_DOC_FIELD_PATH).getValue());

      // Check if the internal "$v" field is contained which was added to the
      // oplog format in 3.6+ If so, then we simply remove it for now since
      // it's not used by the sink connector at the moment and would break
      // CDC-mode based "replication".
      updateDoc.remove(INTERNAL_OPLOG_FIELD_V);

      // patch contains full new document for replacement
      if (updateDoc.containsKey(ID_FIELD)) {
        BsonDocument filterDoc = new BsonDocument(ID_FIELD, updateDoc.get(ID_FIELD));
        return new ReplaceOneModel<>(filterDoc, updateDoc, REPLACE_OPTIONS);
      }

      // patch contains idempotent change only to update original document with
      BsonDocument keyDoc =
          doc.getKeyDoc()
              .orElseThrow(
                  () -> new DataException("Key document must not be missing for update operation"));

      if (!keyDoc.containsKey(JSON_ID_FIELD)) {
        throw new DataException(format("Update document missing `%s` field.", JSON_ID_FIELD));
      }

      BsonDocument filterDoc =
          BsonDocument.parse(
              format("{%s: %s}", ID_FIELD, keyDoc.getString(JSON_ID_FIELD).getValue()));
      return new UpdateOneModel<>(filterDoc, updateDoc);

    } catch (DataException exc) {
      throw exc;
    } catch (Exception exc) {
      throw new DataException(exc.getMessage(), exc);
    }
  }
}
