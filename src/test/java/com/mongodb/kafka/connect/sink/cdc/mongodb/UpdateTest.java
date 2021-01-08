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

package com.mongodb.kafka.connect.sink.cdc.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Update;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class UpdateTest {
  private static final Update UPDATE = new Update();
  private static final BsonDocument CHANGE_EVENT =
      BsonDocument.parse(
          "{_id: {\"_id\": {\"_data\": \"5f15aab12435743f9bd126a4\"} },"
              + "   operationType: 'update',"
              + "   clusterTime: {\"$timestamp\": {\"t\": 123456789, \"i\": 42}},"
              + "   ns: {"
              + "      db: 'engineering',"
              + "      coll: 'users'"
              + "   },"
              + "   documentKey: {"
              + "      _id: {\"$oid\": \"599af247bb69cd89961c986d\"}"
              + "   },"
              + "   updateDescription: {"
              + "      updatedFields: {"
              + "         email: 'alice@10gen.com'"
              + "      },"
              + "      removedFields: ['phoneNumber'],"
              + "      truncatedArrays: [{field: \"arrayField\", newSize: 2}],"
              + "   },"
              + "   fullDocument: {"
              + "      _id: ObjectId(\"58a4eb4a30c75625e00d2820\"),"
              + "      name: 'Alice',"
              + "      userName: 'alice123',"
              + "      email: 'alice@10gen.com',"
              + "      team: 'replication'"
              + "   }"
              + "}");

  @Test
  @DisplayName("when valid cdc event then correct ReplaceOneModel")
  void testValidSinkDocumentWithFullDocument() {
    List<WriteModel<BsonDocument>> results = UPDATE.perform(new SinkDocument(null, CHANGE_EVENT));
    assertEquals(1, results.size());

    WriteModel<BsonDocument> result = results.get(0);
    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");
    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(CHANGE_EVENT.getDocument("documentKey"), writeModel.getFilter());
    assertEquals(CHANGE_EVENT.getDocument("fullDocument"), writeModel.getReplacement());
    assertFalse(
        writeModel.getReplaceOptions().isUpsert(),
        "update replacement expected not to be in upsert mode");
  }

  @Test
  @DisplayName("when valid cdc event then correct UpdateOne")
  void testValidSinkDocumentWithoutFullDocument() {
    BsonDocument event = CHANGE_EVENT.clone();
    event.remove("fullDocument");
    List<WriteModel<BsonDocument>> updates = UPDATE.perform(new SinkDocument(null, event));
    assertEquals(2, updates.size());

    WriteModel<BsonDocument> updateOne = updates.get(0);
    assertTrue(updateOne instanceof UpdateOneModel, "update expected to be of type UpdateOneModel");
    UpdateOneModel<BsonDocument> writeModelOne = (UpdateOneModel<BsonDocument>) updateOne;
    assertTrue(
        writeModelOne.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    BsonDocument push = BsonDocument.parse("{'$push': {'arrayField': {'$each': [], '$slice': 2}}}");
    assertEquals(CHANGE_EVENT.getDocument("documentKey"), writeModelOne.getFilter());
    assertEquals(push, writeModelOne.getUpdate());

    WriteModel<BsonDocument> updateTwo = updates.get(1);
    assertTrue(updateTwo instanceof UpdateOneModel, "update expected to be of type UpdateOneModel");
    UpdateOneModel<BsonDocument> writeModelTwo = (UpdateOneModel<BsonDocument>) updateTwo;
    assertTrue(
        writeModelOne.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    BsonDocument update =
        BsonDocument.parse(
            "{'$set': {'email': 'alice@10gen.com'}," + "'$unset': {'phoneNumber': ''}}}");
    assertEquals(CHANGE_EVENT.getDocument("documentKey"), writeModelTwo.getFilter());
    assertEquals(update, writeModelTwo.getUpdate());
  }

  @Test
  @DisplayName("when missing or incorrect change event data then DataException")
  void testMissingChangeEventData() {
    assertAll(
        "Missing or incorrect change event data",
        () ->
            assertThrows(
                DataException.class,
                () -> UPDATE.perform(new SinkDocument(null, new BsonDocument()))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(null, BsonDocument.parse("{documentKey: {a: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(null, BsonDocument.parse("{fullDocument: {a: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null, BsonDocument.parse("{documentKey: 1, fullDocument: {a: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null, BsonDocument.parse("{documentKey: {}, fullDocument: 1}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null, BsonDocument.parse("{documentKey: {}, updateDescription: 1}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null,
                            BsonDocument.parse(
                                "{documentKey: {}, updateDescription: {updatedFields: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null,
                            BsonDocument.parse(
                                "{documentKey: {}, updateDescription: {removedFields: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null,
                            BsonDocument.parse(
                                "{documentKey: {}, updateDescription: {truncatedArrays: 1}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null,
                            BsonDocument.parse(
                                "{documentKey: {}, updateDescription: {truncatedArrays: [{}]}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null,
                            BsonDocument.parse(
                                "{documentKey: {}, updateDescription: {truncatedArrays: [{field: 1}]}}")))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE.perform(
                        new SinkDocument(
                            null,
                            BsonDocument.parse(
                                "{documentKey: {}, updateDescription: {truncatedArrays: [{field: 'a', newSize: 'a'}]}}")))));
  }
}
