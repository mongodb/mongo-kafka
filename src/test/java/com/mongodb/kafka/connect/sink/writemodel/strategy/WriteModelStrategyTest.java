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

package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialKeyStrategy;

@RunWith(JUnitPlatform.class)
class WriteModelStrategyTest {
  private static final InsertOneDefaultStrategy INSERT_ONE_DEFAULT_STRATEGY =
      new InsertOneDefaultStrategy();
  private static final DeleteOneDefaultStrategy DELETE_ONE_DEFAULT_STRATEGY =
      new DeleteOneDefaultStrategy();
  private static final ReplaceOneDefaultStrategy REPLACE_ONE_DEFAULT_STRATEGY =
      new ReplaceOneDefaultStrategy();
  private static final ReplaceOneBusinessKeyStrategy REPLACE_ONE_BUSINESS_KEY_STRATEGY =
      new ReplaceOneBusinessKeyStrategy();
  private static final UpdateOneTimestampsStrategy UPDATE_ONE_TIMESTAMPS_STRATEGY =
      new UpdateOneTimestampsStrategy();
  private static final UpdateOneBusinessKeyTimestampStrategy
      UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_STRATEGY = new UpdateOneBusinessKeyTimestampStrategy();
  private static final ReplaceOneBusinessKeyStrategy REPLACE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY;
  private static final UpdateOneBusinessKeyTimestampStrategy
      UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_PARTIAL_STRATEGY;
  private static final DeleteOneBusinessKeyStrategy DELETE_ONE_BUSINESS_KEY_STRATEGY =
      new DeleteOneBusinessKeyStrategy();
  private static final DeleteOneBusinessKeyStrategy DELETE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY;
  private static final SinkDocument SINK_DOCUMENT_NULL_VALUE =
      new SinkDocument(new BsonDocument(), null);
  private static final SinkDocument SINK_DOCUMENT_NULL_KEY =
      new SinkDocument(null, new BsonDocument());
  private static final SinkDocument SINK_DOCUMENT_EMPTY =
      new SinkDocument(new BsonDocument(), new BsonDocument());

  static {
    Map<String, String> configMap = createConfigMap();
    configMap.put(
        MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG, PartialKeyStrategy.class.getName());
    configMap.put(
        MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG, "AllowList");

    MongoSinkTopicConfig partialKeyConfig =
        new MongoSinkConfig(configMap).getMongoSinkTopicConfig(TEST_TOPIC);

    REPLACE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY = new ReplaceOneBusinessKeyStrategy();
    REPLACE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.configure(partialKeyConfig);

    UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_PARTIAL_STRATEGY =
        new UpdateOneBusinessKeyTimestampStrategy();
    UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_PARTIAL_STRATEGY.configure(partialKeyConfig);
    DELETE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY = new DeleteOneBusinessKeyStrategy();
    DELETE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.configure(partialKeyConfig);
  }

  private static final BsonDocument VALUE_DOC =
      BsonDocument.parse(
          "{_id: {a: {a1: 1}, b: {b1: 1, b2: 1}}, a: {a1: 1}, b: {b1: 1, b2: 1, c1: 1}}");

  private static final BsonDocument REPLACEMENT_DOC =
      BsonDocument.parse("{a: {a1: 1}, b: {b1: 1, b2: 1, c1: 1}}");

  private static final BsonDocument ID_FILTER =
      BsonDocument.parse("{_id: {a: {a1: 1}, b: {b1: 1, b2: 1}}}");

  private static final BsonDocument BUSINESS_KEY_FILTER =
      BsonDocument.parse("{a: {a1: 1}, b: {b1: 1, b2: 1}}");

  private static final BsonDocument BUSINESS_KEY_FLATTENED_FILTER =
      BsonDocument.parse("{'a.a1': 1, 'b.b1': 1, 'b.b2': 1}");

  @Test
  @DisplayName("Ensure default write model strategy sets the expected WriteModelStrategy")
  void testDefaultWriteModelStrategy() {
    DefaultWriteModelStrategy defaultWriteModelStrategy = new DefaultWriteModelStrategy();
    MongoSinkTopicConfig topicConfig = createTopicConfig();

    assertTrue(
        defaultWriteModelStrategy.getWriteModelStrategy() instanceof ReplaceOneDefaultStrategy);

    defaultWriteModelStrategy.configure(topicConfig);
    assertTrue(
        defaultWriteModelStrategy.getWriteModelStrategy() instanceof ReplaceOneDefaultStrategy);

    topicConfig = createTopicConfig(MongoSinkTopicConfig.TIMESERIES_TIMEFIELD_CONFIG, "ts");
    defaultWriteModelStrategy.configure(topicConfig);
    assertTrue(
        defaultWriteModelStrategy.getWriteModelStrategy() instanceof InsertOneDefaultStrategy);
  }

  @Test
  @DisplayName(
      "when sink document is valid for InsertOneDefaultStrategy then correct InsertOneModel")
  void testInsertOneDefaultStrategyWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        INSERT_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof InsertOneModel, "result expected to be of type InsertOneModel");

    InsertOneModel<BsonDocument> writeModel = (InsertOneModel<BsonDocument>) result;
    assertEquals(VALUE_DOC, writeModel.getDocument(), "Insert doc not matching what is expected");
  }

  @Test
  @DisplayName(
      "when sink document is valid for DeleteOneDefaultStrategy then correct DeleteOneModel")
  void testDeleteOneDefaultStrategyWitValidSinkDocument() {
    BsonDocument keyDoc = BsonDocument.parse("{id: 1234}");

    WriteModel<BsonDocument> result =
        DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(keyDoc, null));

    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");

    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;

    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");

    assertEquals(BsonDocument.parse("{_id: {id: 1234}}"), writeModel.getFilter());
  }

  @Test
  @DisplayName(
      "when sink document is valid for ReplaceOneDefaultStrategy then correct ReplaceOneModel")
  void testReplaceOneDefaultStrategyWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;

    assertEquals(
        VALUE_DOC, writeModel.getReplacement(), "replacement doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");
    assertEquals(ID_FILTER, writeModel.getFilter());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }

  @Test
  @DisplayName(
      "when sink document is valid for ReplaceOneBusinessKeyStrategy then correct ReplaceOneModel")
  void testReplaceOneBusinessKeyStrategyWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
            new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;

    assertEquals(
        REPLACEMENT_DOC,
        writeModel.getReplacement(),
        "replacement doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");

    assertEquals(BUSINESS_KEY_FILTER, writeModel.getFilter());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }

  @Test
  @DisplayName(
      "when sink document is valid for ReplaceOneBusinessKeyStrategy with partial id strategy then correct ReplaceOneModel")
  void testReplaceOneBusinessKeyStrategyPartialWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        REPLACE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.createWriteModel(
            new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof ReplaceOneModel, "result expected to be of type ReplaceOneModel");

    ReplaceOneModel<BsonDocument> writeModel = (ReplaceOneModel<BsonDocument>) result;

    assertEquals(
        REPLACEMENT_DOC,
        writeModel.getReplacement(),
        "replacement doc not matching what is expected");
    assertTrue(
        writeModel.getFilter() instanceof BsonDocument,
        "filter expected to be of type BsonDocument");

    assertEquals(BUSINESS_KEY_FLATTENED_FILTER, writeModel.getFilter());
    assertTrue(
        writeModel.getReplaceOptions().isUpsert(),
        "replacement expected to be done in upsert mode");
  }

  @Test
  @DisplayName(
      "when sink document is valid for UpdateOneTimestampsStrategy then correct UpdateOneModel")
  void testUpdateOneTimestampsStrategyWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

    UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;

    // NOTE: This test case can only check:
    // i) for both fields to be available
    // ii) having the correct BSON type (BsonDateTime)
    // iii) and be initially equal
    // The exact dateTime value is not directly testable here.
    BsonDocument updateDoc = (BsonDocument) writeModel.getUpdate();
    assertNotNull(updateDoc);

    BsonDocument setDocument = updateDoc.getDocument("$set", new BsonDocument());
    BsonDocument setOnInsert = updateDoc.getDocument("$setOnInsert", new BsonDocument());

    BsonDateTime modifiedTS =
        setDocument.getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_MODIFIED_TS);
    BsonDateTime insertedTS =
        setOnInsert.getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_INSERTED_TS);

    assertEquals(
        insertedTS, modifiedTS, "modified and inserted timestamps must initially be equal");
    assertEquals(ID_FILTER, writeModel.getFilter());

    setOnInsert.remove(UpdateOneTimestampsStrategy.FIELD_NAME_INSERTED_TS);
    assertTrue(setOnInsert.isEmpty());

    setDocument.remove(UpdateOneTimestampsStrategy.FIELD_NAME_MODIFIED_TS);
    assertEquals(setDocument, VALUE_DOC);

    assertTrue(writeModel.getOptions().isUpsert(), "update expected to be done in upsert mode");
  }

  @Test
  @DisplayName(
      "when sink document is valid for UpdateOneBusinessKeyTimestampStrategy then correct UpdateOneModel")
  void testUpdateOneBusinessKeyTimestampsStrategyWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_STRATEGY.createWriteModel(
            new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

    UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;

    // NOTE: This test case can only check:
    // i) for both fields to be available
    // ii) having the correct BSON type (BsonDateTime)
    // iii) and be initially equal
    // The exact dateTime value is not directly testable here.
    BsonDocument updateDoc = (BsonDocument) writeModel.getUpdate();
    assertNotNull(updateDoc);

    BsonDocument setDocument = updateDoc.getDocument("$set", new BsonDocument());
    BsonDocument setOnInsert = updateDoc.getDocument("$setOnInsert", new BsonDocument());

    BsonDateTime modifiedTS =
        setDocument.getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_MODIFIED_TS);
    BsonDateTime insertedTS =
        setOnInsert.getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_INSERTED_TS);

    assertEquals(
        insertedTS, modifiedTS, "modified and inserted timestamps must initially be equal");
    assertEquals(BUSINESS_KEY_FILTER, writeModel.getFilter());

    setOnInsert.remove(UpdateOneTimestampsStrategy.FIELD_NAME_INSERTED_TS);
    assertTrue(setOnInsert.isEmpty());

    setDocument.remove(UpdateOneTimestampsStrategy.FIELD_NAME_MODIFIED_TS);
    assertEquals(setDocument, REPLACEMENT_DOC);

    assertTrue(writeModel.getOptions().isUpsert(), "update expected to be done in upsert mode");
  }

  @Test
  @DisplayName(
      "when sink document is valid for UpdateOneBusinessKeyTimestampStrategy with partial id strategy then correct UpdateOneModel")
  void testUpdateOneBusinessKeyTimestampsStrategyPartialWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_PARTIAL_STRATEGY.createWriteModel(
            new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof UpdateOneModel, "result expected to be of type UpdateOneModel");

    UpdateOneModel<BsonDocument> writeModel = (UpdateOneModel<BsonDocument>) result;

    // NOTE: This test case can only check:
    // i) for both fields to be available
    // ii) having the correct BSON type (BsonDateTime)
    // iii) and be initially equal
    // The exact dateTime value is not directly testable here.
    BsonDocument updateDoc = (BsonDocument) writeModel.getUpdate();
    assertNotNull(updateDoc);

    BsonDocument setDocument = updateDoc.getDocument("$set", new BsonDocument());
    BsonDocument setOnInsert = updateDoc.getDocument("$setOnInsert", new BsonDocument());

    BsonDateTime modifiedTS =
        setDocument.getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_MODIFIED_TS);
    BsonDateTime insertedTS =
        setOnInsert.getDateTime(UpdateOneTimestampsStrategy.FIELD_NAME_INSERTED_TS);

    assertEquals(
        insertedTS, modifiedTS, "modified and inserted timestamps must initially be equal");
    assertEquals(BUSINESS_KEY_FLATTENED_FILTER, writeModel.getFilter());

    setOnInsert.remove(UpdateOneTimestampsStrategy.FIELD_NAME_INSERTED_TS);
    assertTrue(setOnInsert.isEmpty());

    setDocument.remove(UpdateOneTimestampsStrategy.FIELD_NAME_MODIFIED_TS);
    assertEquals(setDocument, REPLACEMENT_DOC);

    assertTrue(writeModel.getOptions().isUpsert(), "update expected to be done in upsert mode");
  }

  @Test
  @DisplayName(
      "when sink document is valid for UpdateOneBusinessKeyTimestampStrategy then correct UpdateOneModel")
  void testDeleteOneBusinessKeyStrategyWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        DELETE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(
            new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");

    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;
    assertEquals(VALUE_DOC.get("_id"), writeModel.getFilter());
  }

  @Test
  @DisplayName(
      "when sink document is valid for UpdateOneBusinessKeyTimestampStrategy with partial id strategy then correct UpdateOneModel")
  void testDeleteOneBusinessKeyStrategyStrategyPartialWithValidSinkDocument() {
    WriteModel<BsonDocument> result =
        DELETE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.createWriteModel(
            new SinkDocument(null, VALUE_DOC.clone()));
    assertTrue(result instanceof DeleteOneModel, "result expected to be of type DeleteOneModel");

    DeleteOneModel<BsonDocument> writeModel = (DeleteOneModel<BsonDocument>) result;
    assertEquals(BUSINESS_KEY_FLATTENED_FILTER, writeModel.getFilter());
  }

  @Test
  @DisplayName("Test handling empty or missing sink document data")
  void testIEmptyOrMissingSinkDocumentData() {
    assertAll(
        () ->
            assertThrows(
                DataException.class,
                () -> DELETE_ONE_DEFAULT_STRATEGY.createWriteModel(SINK_DOCUMENT_NULL_KEY)),
        () ->
            assertThrows(
                DataException.class,
                () -> REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () -> REPLACE_ONE_DEFAULT_STRATEGY.createWriteModel(SINK_DOCUMENT_EMPTY)),
        () ->
            assertThrows(
                DataException.class,
                () -> REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () -> REPLACE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(SINK_DOCUMENT_EMPTY)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    REPLACE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    REPLACE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_EMPTY)),
        () ->
            assertThrows(
                DataException.class,
                () -> UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () -> UPDATE_ONE_TIMESTAMPS_STRATEGY.createWriteModel(SINK_DOCUMENT_EMPTY)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_EMPTY)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_PARTIAL_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    UPDATE_ONE_BUSINESS_KEY_TIMESTAMPS_PARTIAL_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_EMPTY)),
        () ->
            assertThrows(
                DataException.class,
                () -> DELETE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () -> DELETE_ONE_BUSINESS_KEY_STRATEGY.createWriteModel(SINK_DOCUMENT_EMPTY)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    DELETE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_NULL_VALUE)),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    DELETE_ONE_BUSINESS_KEY_PARTIAL_STRATEGY.createWriteModel(
                        SINK_DOCUMENT_EMPTY)));
  }
}
