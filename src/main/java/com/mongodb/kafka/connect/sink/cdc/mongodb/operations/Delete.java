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

package com.mongodb.kafka.connect.sink.cdc.mongodb.operations;

import static com.mongodb.kafka.connect.sink.cdc.mongodb.operations.OperationHelper.getDocumentKey;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class Delete implements CdcOperation {

  @Override
  public WriteModel<BsonDocument> perform(final SinkDocument doc) {
    BsonDocument changeStreamDocument =
        doc.getValueDoc()
            .orElseThrow(
                () ->
                    new DataException("Error: value doc must not be missing for delete operation"));
    return new DeleteOneModel<>(getDocumentKey(changeStreamDocument));
  }
}
