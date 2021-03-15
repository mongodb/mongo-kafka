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

package com.mongodb.kafka.connect.sink.cdc.mongodb.operations;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class ReplaceTest {
  private static final Replace REPLACE = new Replace();
  private static final BsonDocument CHANGE_EVENT =
      BsonDocument.parse(
          "{_id: {\"_id\": {\"_data\": \"5f15aab12435743f9bd126a4\"} },"
              + "   operationType: 'replace',"
              + "   clusterTime: {\"$timestamp\": {\"t\": 123456789, \"i\": 42}},"
              + "   ns: {"
              + "      db: 'engineering',"
              + "      coll: 'users'"
              + "   },"
              + "   documentKey: {"
              + "      _id: {\"$oid\": \"599af247bb69cd89961c986d\"}"
              + "   },"
              + "   fullDocument: {"
              + "      _id: {\"$oid\": \"599af247bb69cd89961c986d\"},"
              + "      userName: 'alice123',"
              + "      name: 'Alice'"
              + "   }"
              + "}");

  @Test
  @DisplayName("when valid cdc event then correct ReplaceOneModel")
  void testValidSinkDocument() {
    WriteModel<BsonDocument> result = REPLACE.perform(new SinkDocument(null, CHANGE_EVENT));
    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");
    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(CHANGE_EVENT.getDocument("documentKey"), writeModel.getFilter());
    assertEquals(CHANGE_EVENT.getDocument("fullDocument"), writeModel.getReplacement());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }

  @Test
  @DisplayName("when missing or incorrect change event data then DataException")
  void testMissingChangeEventData() {
    assertAll(
        "Missing or incorrect change event data",
        () ->
            assertThrows(
                DataException.class,
                () -> REPLACE.perform(new SinkDocument(null, new BsonDocument()))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    REPLACE.perform(
                        new SinkDocument(null, BsonDocument.parse("{documentKey: {a: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    REPLACE.perform(
                        new SinkDocument(null, BsonDocument.parse("{fullDocument: {a: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    REPLACE.perform(
                        new SinkDocument(
                            null, BsonDocument.parse("{documentKey: 1, fullDocument: {a: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    REPLACE.perform(
                        new SinkDocument(
                            null, BsonDocument.parse("{documentKey: {}, fullDocument: 1}")))));
  }
}
