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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;
import org.bson.BsonString;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class MongoDbDeleteTest {
  private static final MongoDbDelete DELETE = new MongoDbDelete();
  private static final BsonDocument FILTER_DOC = BsonDocument.parse("{_id: 1234}");

  @Test
  @DisplayName("when valid cdc event then correct DeleteOneModel")
  void testValidSinkDocument() {
    BsonDocument keyDoc = BsonDocument.parse("{id: '1234'}");
    WriteModel<BsonDocument> result = DELETE.perform(new SinkDocument(keyDoc, null));

    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");
    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(FILTER_DOC, writeModel.getFilter());
  }

  @Test
  @DisplayName("when missing key doc then DataException")
  void testMissingKeyDocument() {
    assertThrows(
        DataException.class, () -> DELETE.perform(new SinkDocument(null, new BsonDocument())));
  }

  @Test
  @DisplayName("when key doc 'id' field not of type String then DataException")
  void testInvalidTypeIdFieldInKeyDocument() {
    BsonDocument keyDoc = BsonDocument.parse("{id: 1234}");
    assertThrows(
        DataException.class, () -> DELETE.perform(new SinkDocument(keyDoc, new BsonDocument())));
  }

  @Test
  @DisplayName("when key doc 'id' field contains invalid JSON then DataException")
  void testInvalidJsonIdFieldInKeyDocument() {
    BsonDocument keyDoc = new BsonDocument("id", new BsonString("{,NOT:JSON,}"));
    assertThrows(
        DataException.class, () -> DELETE.perform(new SinkDocument(keyDoc, new BsonDocument())));
  }
}
