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

package com.mongodb.kafka.connect.sink.cdc.debezium.rdbms;

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
class RdbmsInsertTest {
  private static final RdbmsInsert RDBMS_INSERT = new RdbmsInsert();

  @Test
  @DisplayName("when valid cdc event with single field PK then correct ReplaceOneModel")
  void testValidSinkDocumentSingleFieldPK() {
    BsonDocument filterDoc = BsonDocument.parse("{_id: {id: 1234}}");
    BsonDocument replacementDoc =
        BsonDocument.parse("{_id: {id: 1234}, first_name: 'Grace', last_name: 'Hopper'}");
    BsonDocument keyDoc = BsonDocument.parse("{id: 1234}");
    BsonDocument valueDoc =
        BsonDocument.parse(
            "{op: 'c', after: {id: 1234, first_name: 'Grace', last_name: 'Hopper'}}");

    WriteModel<BsonDocument> result = RDBMS_INSERT.perform(new SinkDocument(keyDoc, valueDoc));
    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;
    assertEquals(
        replacementDoc,
        writeModel.getReplacement(),
        "replacement doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(filterDoc, writeModel.getFilter());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }

  @Test
  @DisplayName("when valid cdc event with compound PK then correct ReplaceOneModel")
  void testValidSinkDocumentCompoundPK() {
    BsonDocument filterDoc = BsonDocument.parse("{_id: {idA: 123, idB: 'ABC'}}");
    BsonDocument replacementDoc = BsonDocument.parse("{_id: {idA: 123, idB: 'ABC'}, active: true}");
    BsonDocument keyDoc = BsonDocument.parse("{idA: 123, idB: 'ABC'}");
    BsonDocument valueDoc =
        BsonDocument.parse("{op: 'c', after: {_id: {idA: 123, idB: 'ABC'}, active: true}}");

    WriteModel<BsonDocument> result = RDBMS_INSERT.perform(new SinkDocument(keyDoc, valueDoc));
    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;
    assertEquals(
        replacementDoc,
        writeModel.getReplacement(),
        "replacement doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(filterDoc, writeModel.getFilter());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }

  @Test
  @DisplayName("when valid cdc event without PK then correct ReplaceOneModel")
  void testValidSinkDocumentNoPK() {
    BsonDocument valueDocCreate =
        BsonDocument.parse("{op: 'c', after: {text: 'misc', active: false}}");
    verifyResultsNoPK(valueDocCreate);

    BsonDocument valueDocRead =
        BsonDocument.parse("{op: 'r', after: {text: 'misc', active: false}}");
    verifyResultsNoPK(valueDocRead);
  }

  @Test
  @DisplayName("when missing key doc then DataException")
  void testMissingKeyDocument() {
    assertThrows(
        DataException.class,
        () -> RDBMS_INSERT.perform(new SinkDocument(null, new BsonDocument())));
  }

  @Test
  @DisplayName("when missing value doc then DataException")
  void testMissingValueDocument() {
    assertThrows(
        DataException.class,
        () -> RDBMS_INSERT.perform(new SinkDocument(new BsonDocument(), null)));
  }

  @Test
  @DisplayName("when invalid json in value doc 'after' field then DataException")
  void testInvalidAfterField() {
    assertThrows(
        DataException.class,
        () ->
            RDBMS_INSERT.perform(
                new SinkDocument(
                    new BsonDocument(),
                    BsonDocument.parse("{op: 'c', after: '{MAL: FORMED [JSON]}'}"))));
  }

  private void verifyResultsNoPK(final BsonDocument valueDoc) {
    // NOTE: for both filterDoc and replacementDoc _id have a generated ObjectId fetched from the
    // WriteModel
    BsonDocument filterDoc = new BsonDocument();
    BsonDocument keyDoc = new BsonDocument();
    BsonDocument replacementDoc = valueDoc.getDocument("after").clone();

    WriteModel<BsonDocument> result = RDBMS_INSERT.perform(new SinkDocument(keyDoc, valueDoc));

    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getReplacement().isObjectId("_id"),
        "replacement doc must contain _id field of type ObjectID");

    replacementDoc.put("_id", writeModel.getReplacement().getObjectId("_id"));
    assertEquals(
        replacementDoc,
        writeModel.getReplacement(),
        "replacement doc not matching what is expected");

    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertTrue(
        ((BsonDocument) writeModel.getFilter()).isObjectId("_id"),
        "filter doc must contain _id field of type ObjectID");

    filterDoc.put("_id", ((BsonDocument) writeModel.getFilter()).getObjectId("_id"));
    assertEquals(filterDoc, writeModel.getFilter());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }
}
