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

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class MongoDbInsertTest {
  private static final MongoDbInsert INSERT = new MongoDbInsert();
  private static final BsonDocument FILTER_DOC = BsonDocument.parse("{_id: 1234}");
  private static final BsonDocument REPLACEMENT_DOC =
      BsonDocument.parse("{_id: 1234, first_name: 'Grace', last_name: 'Hopper'}");

  @Test
  @DisplayName("when valid cdc event then correct ReplaceOneModel")
  void testValidSinkDocument() {
    BsonDocument keyDoc = new BsonDocument("id", new BsonString("1234"));
    BsonDocument valueDoc =
        new BsonDocument("op", new BsonString("c"))
            .append("after", new BsonString(REPLACEMENT_DOC.toJson()));

    WriteModel<BsonDocument> result = INSERT.perform(new SinkDocument(keyDoc, valueDoc));

    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;

    assertEquals(
        REPLACEMENT_DOC,
        writeModel.getReplacement(),
        "replacement doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(FILTER_DOC, writeModel.getFilter());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }

  @Test
  @DisplayName("when missing value doc then DataException")
  void testMissingValueDocument() {
    assertThrows(
        DataException.class, () -> INSERT.perform(new SinkDocument(new BsonDocument(), null)));
  }

  @Test
  @DisplayName("when invalid json in value doc 'after' field then DataException")
  void testInvalidAfterField() {
    assertThrows(
        DataException.class,
        () ->
            INSERT.perform(
                new SinkDocument(
                    new BsonDocument(),
                    BsonDocument.parse("{op: 'c', after: '{MAL: FORMED [JSON]}'}"))));
  }
}
