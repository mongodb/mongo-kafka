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

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class RdbmsDeleteTest {
  private static final RdbmsDelete RDBMS_DELETE = new RdbmsDelete();

  @Test
  @DisplayName("when valid cdc event with single field PK then correct DeleteOneModel")
  void testValidSinkDocumentSingleFieldPK() {
    BsonDocument filterDoc = BsonDocument.parse("{_id: {id: 1004}}");
    BsonDocument keyDoc = BsonDocument.parse("{id: 1004}");
    BsonDocument valueDoc = BsonDocument.parse("{op: 'd'}");

    WriteModel<BsonDocument> result = RDBMS_DELETE.perform(new SinkDocument(keyDoc, valueDoc));
    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");

    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(filterDoc, writeModel.getFilter());
  }

  @Test
  @DisplayName("when valid cdc event with compound PK then correct DeleteOneModel")
  void testValidSinkDocumentCompoundPK() {
    BsonDocument filterDoc = BsonDocument.parse("{_id: {idA: 123, idB: 'ABC'}}");
    BsonDocument keyDoc = BsonDocument.parse("{idA: 123, idB: 'ABC'}");
    BsonDocument valueDoc = BsonDocument.parse("{op: 'd'}");

    WriteModel<BsonDocument> result = RDBMS_DELETE.perform(new SinkDocument(keyDoc, valueDoc));
    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");

    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(filterDoc, writeModel.getFilter());
  }

  @Test
  @DisplayName("when valid cdc event without PK then correct DeleteOneModel")
  void testValidSinkDocumentNoPK() {
    BsonDocument filterDoc = BsonDocument.parse("{text: 'misc', number: 9876, active: true}");
    BsonDocument keyDoc = new BsonDocument();
    BsonDocument valueDoc =
        BsonDocument.parse("{op: 'c', before: {text: 'misc', number: 9876, active: true}}");

    WriteModel<BsonDocument> result = RDBMS_DELETE.perform(new SinkDocument(keyDoc, valueDoc));
    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");

    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(filterDoc, writeModel.getFilter());
  }

  @Test
  @DisplayName("when missing key doc then DataException")
  void testMissingKeyDocument() {
    assertThrows(
        DataException.class,
        () -> RDBMS_DELETE.perform(new SinkDocument(null, new BsonDocument())));
  }

  @Test
  @DisplayName("when missing value doc then DataException")
  void testMissingValueDocument() {
    assertThrows(
        DataException.class,
        () -> RDBMS_DELETE.perform(new SinkDocument(new BsonDocument(), null)));
  }

  @Test
  @DisplayName("when key doc and value 'before' field both empty then DataException")
  void testEmptyKeyDocAndEmptyValueBeforeField() {
    assertThrows(
        DataException.class,
        () ->
            RDBMS_DELETE.perform(
                new SinkDocument(new BsonDocument(), BsonDocument.parse("{op: 'd', before: {}}"))));
  }
}
