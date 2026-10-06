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
package com.mongodb.kafka.connect.sink.cdc;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.mongodb.MongoDBHelper;
import com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbDelete;
import com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbInsert;
import com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbUpdate;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Delete;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

/**
 * Verifies that CDC handlers emit equality filters on {@code _id} that cannot be reinterpreted as
 * query operators by the server, so a forged or operator-shaped identifier can never match (and
 * delete / replace) an unintended document.
 */
class CdcSinkFilterInjectionIntegrationTest {
  @RegisterExtension public static final MongoDBHelper MONGODB = new MongoDBHelper();

  private static final String COLL = "cdcFilterInjection";

  private MongoCollection<BsonDocument> coll;

  @BeforeEach
  void setUp() {
    coll = MONGODB.getDatabase().getCollection(COLL, BsonDocument.class);
    coll.deleteMany(new BsonDocument());
  }

  @Test
  @DisplayName("forged operator id in debezium delete event does not delete other documents")
  void testDebeziumDeleteForgedOperatorId() {
    coll.insertMany(
        asList(
            BsonDocument.parse("{_id: 1, s: 'victim-1'}"),
            BsonDocument.parse("{_id: 2, s: 'victim-2'}")));

    SinkDocument event =
        new SinkDocument(BsonDocument.parse("{id: '{\"$ne\": null}'}"), new BsonDocument());
    WriteModel<BsonDocument> model = new MongoDbDelete().perform(event);

    // Older servers no-op on the forged filter; newer ones reject the malformed _id outright
    try {
      BulkWriteResult result = coll.bulkWrite(singletonList(model));
      assertEquals(0, result.getDeletedCount());
    } catch (MongoBulkWriteException ignored) {
    }

    assertEquals(2, coll.countDocuments(new BsonDocument()));
  }

  @Test
  @DisplayName("forged operator id in debezium insert event does not replace other documents")
  void testDebeziumInsertForgedOperatorId() {
    coll.insertMany(
        asList(
            BsonDocument.parse("{_id: 1, s: 'original-1'}"),
            BsonDocument.parse("{_id: 2, s: 'original-2'}")));

    SinkDocument event =
        new SinkDocument(
            BsonDocument.parse("{id: '{\"$ne\": null}'}"),
            BsonDocument.parse("{after: '{_id: {\"$ne\": null}, s: \"forged\"}'}"));
    WriteModel<BsonDocument> model = new MongoDbInsert().perform(event);

    // The server may reject the forged upsert outright; either way no victim is replaced
    try {
      BulkWriteResult result = coll.bulkWrite(singletonList(model));
      assertEquals(0, result.getMatchedCount());
    } catch (MongoBulkWriteException ignored) {
    }

    assertEquals(
        "original-1",
        coll.find(BsonDocument.parse("{_id: 1}")).first().get("s").asString().getValue());
    assertEquals(
        "original-2",
        coll.find(BsonDocument.parse("{_id: 2}")).first().get("s").asString().getValue());
  }

  @Test
  @DisplayName("forged operator id in debezium update event does not replace other documents")
  void testDebeziumUpdateForgedOperatorId() {
    coll.insertMany(
        asList(
            BsonDocument.parse("{_id: 1, s: 'original-1'}"),
            BsonDocument.parse("{_id: 2, s: 'original-2'}")));

    SinkDocument event =
        new SinkDocument(
            BsonDocument.parse("{id: '{\"$ne\": null}'}"),
            BsonDocument.parse("{after: '{s: \"forged\"}'}"));
    WriteModel<BsonDocument> model =
        new MongoDbUpdate(MongoDbUpdate.EventFormat.ChangeStream).perform(event);

    // The server rejects the upsert rather than replacing an arbitrary document
    assertThrows(MongoBulkWriteException.class, () -> coll.bulkWrite(singletonList(model)));

    assertEquals(
        "original-1",
        coll.find(BsonDocument.parse("{_id: 1}")).first().get("s").asString().getValue());
    assertEquals(
        "original-2",
        coll.find(BsonDocument.parse("{_id: 2}")).first().get("s").asString().getValue());
  }

  @Test
  @DisplayName(
      "forged operator documentKey in change stream delete event does not delete other documents")
  void testChangeStreamDeleteForgedOperatorId() {
    coll.insertMany(
        asList(
            BsonDocument.parse("{_id: 1, s: 'victim-1'}"),
            BsonDocument.parse("{_id: 2, s: 'victim-2'}")));

    SinkDocument event =
        new SinkDocument(
            null,
            BsonDocument.parse("{operationType: 'delete', documentKey: {_id: {'$ne': null}}}"));
    WriteModel<BsonDocument> model = new Delete().perform(event);

    // Older servers no-op on the forged filter; newer ones reject the malformed _id outright
    try {
      BulkWriteResult result = coll.bulkWrite(singletonList(model));
      assertEquals(0, result.getDeletedCount());
    } catch (MongoBulkWriteException ignored) {
    }

    assertEquals(2, coll.countDocuments(new BsonDocument()));
  }

  @Test
  @DisplayName(
      "forged operator field name in change stream delete event is rejected connector-side")
  void testChangeStreamDeleteForgedOperatorFieldName() {
    SinkDocument event =
        new SinkDocument(
            null,
            BsonDocument.parse(
                "{operationType: 'delete', documentKey: {_id: 1, $expr: [true, true]}}"));

    assertThrows(DataException.class, () -> new Delete().perform(event));
  }

  @Test
  @DisplayName("legitimate debezium delete by string id still deletes the correct document")
  void testDebeziumDeleteLegitimateId() {
    coll.insertMany(
        asList(BsonDocument.parse("{_id: 'mykey'}"), BsonDocument.parse("{_id: 'other'}")));

    SinkDocument event =
        new SinkDocument(BsonDocument.parse("{id: '\"mykey\"'}"), new BsonDocument());
    WriteModel<BsonDocument> model = new MongoDbDelete().perform(event);

    coll.bulkWrite(singletonList(model));

    assertEquals(1, coll.countDocuments(new BsonDocument()));
    assertNull(coll.find(BsonDocument.parse("{_id: 'mykey'}")).first());
  }

  @Test
  @DisplayName("legitimate change stream update still replaces the correct document")
  void testChangeStreamUpdateLegitimateId() {
    coll.insertMany(
        asList(
            BsonDocument.parse("{_id: 1, s: 'original'}"),
            BsonDocument.parse("{_id: 2, s: 'untouched'}")));

    SinkDocument event =
        new SinkDocument(
            null,
            BsonDocument.parse(
                "{operationType: 'update', documentKey: {_id: 1}, updateDescription: "
                    + "{updatedFields: {s: 'updated'}, removedFields: [], truncatedArrays: []}}"));
    WriteModel<BsonDocument> model =
        new com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Update().perform(event);

    coll.bulkWrite(singletonList(model));

    assertEquals(
        "updated",
        coll.find(BsonDocument.parse("{_id: 1}")).first().get("s").asString().getValue());
    assertEquals(
        "untouched",
        coll.find(BsonDocument.parse("{_id: 2}")).first().get("s").asString().getValue());
  }
}
