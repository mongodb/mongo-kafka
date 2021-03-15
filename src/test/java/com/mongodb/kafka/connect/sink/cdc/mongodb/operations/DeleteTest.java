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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class DeleteTest {
  private static final Delete DELETE = new Delete();
  private static final BsonDocument CHANGE_EVENT =
      BsonDocument.parse(
          "{_id: {\"_id\": {\"_data\": \"5f15aab12435743f9bd126a4\"} },"
              + "   operationType: 'delete',"
              + "   clusterTime: {\"$timestamp\": {\"t\": 123456789, \"i\": 42}},"
              + "   ns: {"
              + "      db: 'engineering',"
              + "      coll: 'users'"
              + "   },"
              + "   documentKey: {"
              + "      _id: {\"$oid\": \"599af247bb69cd89961c986d\"}"
              + "   }"
              + "}");

  @Test
  @DisplayName("when valid cdc event then correct DeleteOneModel")
  void testValidSinkDocument() {
    WriteModel<BsonDocument> result = DELETE.perform(new SinkDocument(null, CHANGE_EVENT));
    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");
    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(CHANGE_EVENT.getDocument("documentKey"), writeModel.getFilter());
  }

  @Test
  @DisplayName("when missing document key then DataException")
  void testMissingChangeEventDocument() {
    assertThrows(
        DataException.class, () -> DELETE.perform(new SinkDocument(null, new BsonDocument())));
  }
}
