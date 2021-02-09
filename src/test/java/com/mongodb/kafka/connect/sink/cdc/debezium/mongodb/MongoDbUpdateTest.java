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
import org.bson.BsonInt32;
import org.bson.BsonString;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class MongoDbUpdateTest {
  private static final MongoDbUpdate UPDATE = new MongoDbUpdate();
  private static final BsonDocument FILTER_DOC = BsonDocument.parse("{_id: 1234}");
  private static final BsonDocument REPLACEMENT_DOC =
      BsonDocument.parse("{_id: 1234, first_name: 'Grace', last_name: 'Hopper'}");
  private static final BsonDocument UPDATE_DOC =
      BsonDocument.parse("{$set: {first_name: 'Grace', last_name: 'Hopper'}}");
  private static final BsonDocument UPDATE_DOC_WITH_OPLOG_INTERNALS =
      UPDATE_DOC.clone().append("$v", new BsonInt32(1));

  @Test
  @DisplayName("when valid doc replace cdc event then correct ReplaceOneModel")
  void testValidSinkDocumentForReplacement() {

    BsonDocument keyDoc = BsonDocument.parse("{id: 1234}");
    BsonDocument valueDoc =
        new BsonDocument("op", new BsonString("u"))
            .append("patch", new BsonString(REPLACEMENT_DOC.toJson()));

    WriteModel<BsonDocument> result = UPDATE.perform(new SinkDocument(keyDoc, valueDoc));
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
  @DisplayName("when valid doc change cdc event then correct UpdateOneModel")
  void testValidSinkDocumentForUpdate() {
    BsonDocument keyDoc = BsonDocument.parse("{id: '1234'}");
    BsonDocument valueDoc =
        new BsonDocument("op", new BsonString("u"))
            .append("patch", new BsonString(UPDATE_DOC.toJson()));

    WriteModel<BsonDocument> result = UPDATE.perform(new SinkDocument(keyDoc, valueDoc));
    assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

    UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;
    assertEquals(UPDATE_DOC, writeModel.getUpdate(), "update doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(FILTER_DOC, writeModel.getFilter());
  }

  @Test
  @DisplayName(
      "when valid doc change cdc event containing internal oplog fields then correct UpdateOneModel")
  public void testValidSinkDocumentWithInternalOploagFieldForUpdate() {
    BsonDocument keyDoc = BsonDocument.parse("{id: '1234'}");
    BsonDocument valueDoc =
        new BsonDocument("op", new BsonString("u"))
            .append("patch", new BsonString(UPDATE_DOC_WITH_OPLOG_INTERNALS.toJson()));

    WriteModel<BsonDocument> result = UPDATE.perform(new SinkDocument(keyDoc, valueDoc));
    assertTrue(
        result instanceof UpdateOneModel, () -> "result expected to be of type UpdateOneModel");

    UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;
    assertEquals(
        UPDATE_DOC, writeModel.getUpdate(), () -> "update doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        () -> "filter expected to be of type BsonDocument");
    assertEquals(FILTER_DOC, writeModel.getFilter());
  }

  @Test
  @DisplayName("when missing value doc then DataException")
  void testMissingValueDocument() {
    assertThrows(
        DataException.class, () -> UPDATE.perform(new SinkDocument(new BsonDocument(), null)));
  }

  @Test
  @DisplayName("when missing key doc then DataException")
  void testMissingKeyDocument() {
    assertThrows(
        DataException.class,
        () -> UPDATE.perform(new SinkDocument(null, BsonDocument.parse("{patch: {}}"))));
  }

  @Test
  @DisplayName("when 'update' field missing in value doc then DataException")
  void testMissingPatchFieldInValueDocument() {
    assertThrows(
        DataException.class,
        () ->
            UPDATE.perform(
                new SinkDocument(
                    BsonDocument.parse("{id: 1234}"), BsonDocument.parse("{nopatch: {}}"))));
  }

  @Test
  @DisplayName("when 'id' field not of type String in key doc then DataException")
  void testIdFieldNoStringInKeyDocument() {
    assertThrows(
        DataException.class,
        () ->
            UPDATE.perform(
                new SinkDocument(
                    BsonDocument.parse("{id: 1234}"), BsonDocument.parse("{patch: {}}"))));
  }

  @Test
  @DisplayName("when 'id' field invalid JSON in key doc then DataException")
  void testIdFieldInvalidJsonInKeyDocument() {
    assertThrows(
        DataException.class,
        () ->
            UPDATE.perform(
                new SinkDocument(
                    BsonDocument.parse("{id: '{not-Json}'}"), BsonDocument.parse("{patch: {}}"))));
  }
}
