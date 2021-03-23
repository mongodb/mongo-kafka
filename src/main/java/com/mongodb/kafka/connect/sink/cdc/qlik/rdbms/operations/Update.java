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

package com.mongodb.kafka.connect.sink.cdc.qlik.rdbms.operations;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

final class Update implements CdcOperation {
  private static final UpdateOptions UPDATE_OPTIONS = new UpdateOptions().upsert(true);

  @Override
  public WriteModel<BsonDocument> perform(final SinkDocument doc) {
    // patch contains idempotent change only to update original document with
    BsonDocument keyDocument =
        doc.getKeyDoc()
            .orElseThrow(
                () -> new DataException("Error: key doc must not be missing for update operation"));

    BsonDocument valueDocument =
        doc.getValueDoc()
            .orElseThrow(
                () ->
                    new DataException("Error: value doc must not be missing for update operation"));

    BsonDocument filterDocument =
        OperationHelper.createUpdateFilterDocument(keyDocument, valueDocument);
    BsonDocument updateDocument = OperationHelper.createUpdateDocument(valueDocument);
    if (updateDocument.isEmpty()) {
      return null;
    }
    return new UpdateOneModel<>(filterDocument, updateDocument, UPDATE_OPTIONS);
  }
}
