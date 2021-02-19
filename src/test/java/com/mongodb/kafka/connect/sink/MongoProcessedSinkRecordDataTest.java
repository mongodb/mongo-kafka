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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createSinkConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;

import com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler;
import com.mongodb.kafka.connect.sink.namespace.mapping.FieldPathNamespaceMapper;
import com.mongodb.kafka.connect.sink.namespace.mapping.TestNamespaceMapper;

@RunWith(JUnitPlatform.class)
class MongoProcessedSinkRecordDataTest {

  private static final String INSERT_JSON =
      "{_id: 1, first_name: 'Alice', last_name: 'Wonderland'}";
  private static final String VALUE_JSON =
      format(
          "{"
              + "_id: 1, "
              + "op: 'c',"
              + "before: null,"
              + "after: \"%s}\","
              + "source: 'ignored'"
              + "}",
          INSERT_JSON);

  private static final SinkRecord SINK_RECORD =
      new SinkRecord(
          TEST_TOPIC, 0, Schema.STRING_SCHEMA, "{_id: 1}", Schema.STRING_SCHEMA, VALUE_JSON, 1);

  private static final SinkRecord INVALID_SINK_RECORD =
      new SinkRecord(TEST_TOPIC, 0, Schema.INT32_SCHEMA, 1, Schema.INT32_SCHEMA, 1, 1);

  private static final ReplaceOneModel<BsonDocument> EXPECTED_WRITE_MODEL =
      new ReplaceOneModel<>(
          BsonDocument.parse("{_id: 1}"),
          BsonDocument.parse(VALUE_JSON),
          new ReplaceOptions().upsert(true));

  private static final ReplaceOneModel<BsonDocument> CDC_EXPECTED_WRITE_MODEL =
      new ReplaceOneModel<>(
          BsonDocument.parse("{_id: 1}"),
          BsonDocument.parse(INSERT_JSON),
          new ReplaceOptions().upsert(true));

  @Test
  @DisplayName("test default processing")
  void testDefaultProcessing() {
    MongoProcessedSinkRecordData processedData =
        new MongoProcessedSinkRecordData(SINK_RECORD, createSinkConfig());

    assertEquals(new MongoNamespace("myDB.topic"), processedData.getNamespace());
    assertWriteModel(processedData);
  }

  @Test
  @DisplayName("test default processing with collection config")
  void testDefaultProcessingWithCollectionConfig() {
    MongoProcessedSinkRecordData processedData =
        new MongoProcessedSinkRecordData(
            SINK_RECORD, createSinkConfig(COLLECTION_CONFIG, "myColl"));

    assertEquals(new MongoNamespace("myDB.myColl"), processedData.getNamespace());
    assertWriteModel(processedData);
  }

  @Test
  @DisplayName("test custom namespace mapper")
  void testCustomNamespaceMapper() {
    MongoProcessedSinkRecordData processedData =
        new MongoProcessedSinkRecordData(
            SINK_RECORD,
            createSinkConfig(
                NAMESPACE_MAPPER_CONFIG, TestNamespaceMapper.class.getCanonicalName()));

    assertEquals(new MongoNamespace("db.coll.1"), processedData.getNamespace());
    assertWriteModel(processedData);
  }

  @Test
  @DisplayName("test CDC handling")
  void testCDCHandling() {
    MongoProcessedSinkRecordData processedData =
        new MongoProcessedSinkRecordData(
            SINK_RECORD,
            createSinkConfig(
                CHANGE_DATA_CAPTURE_HANDLER_CONFIG, MongoDbHandler.class.getCanonicalName()));

    assertEquals(new MongoNamespace("myDB.topic"), processedData.getNamespace());
    assertWriteModel(processedData, CDC_EXPECTED_WRITE_MODEL);
  }

  @Test
  @DisplayName("test error tolerance is respected")
  void testErrorTolerance() {
    assertAll(
        "Ensure error tolerance is supported",
        () ->
            assertNotNull(
                new MongoProcessedSinkRecordData(
                        INVALID_SINK_RECORD, createSinkConfig(ERRORS_TOLERANCE_CONFIG, "all"))
                    .getException()),
        () ->
            assertNotNull(
                new MongoProcessedSinkRecordData(
                        INVALID_SINK_RECORD,
                        createSinkConfig(
                            format(
                                "{'%s': '%s', '%s': '%s'}",
                                ERRORS_TOLERANCE_CONFIG,
                                "all",
                                CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                                MongoDbHandler.class.getCanonicalName())))
                    .getException()),
        () ->
            assertNotNull(
                new MongoProcessedSinkRecordData(
                        SINK_RECORD,
                        createSinkConfig(
                            format(
                                "{'%s': '%s', '%s': '%s', '%s': 'missingField', '%s': true}",
                                ERRORS_TOLERANCE_CONFIG,
                                "all",
                                NAMESPACE_MAPPER_CONFIG,
                                FieldPathNamespaceMapper.class.getCanonicalName(),
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG,
                                FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG)))
                    .getException()),
        () ->
            assertThrows(
                DataException.class,
                () -> new MongoProcessedSinkRecordData(INVALID_SINK_RECORD, createSinkConfig())),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    new MongoProcessedSinkRecordData(
                        INVALID_SINK_RECORD,
                        createSinkConfig(
                            CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                            MongoDbHandler.class.getCanonicalName()))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    new MongoProcessedSinkRecordData(
                        SINK_RECORD,
                        createSinkConfig(
                            format(
                                "{'%s': '%s', '%s': 'missingField', '%s': true}",
                                NAMESPACE_MAPPER_CONFIG,
                                FieldPathNamespaceMapper.class.getCanonicalName(),
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG,
                                FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG)))));
  }

  void assertWriteModel(final MongoProcessedSinkRecordData processedData) {
    assertWriteModel(processedData, EXPECTED_WRITE_MODEL);
  }

  void assertWriteModel(
      final MongoProcessedSinkRecordData processedData,
      final ReplaceOneModel<BsonDocument> expectedWriteModel) {
    assertNull(processedData.getException());
    ReplaceOneModel<BsonDocument> writeModel =
        (ReplaceOneModel<BsonDocument>) processedData.getWriteModel();
    assertEquals(expectedWriteModel.getFilter(), writeModel.getFilter());
    assertEquals(expectedWriteModel.getReplacement(), writeModel.getReplacement());
    assertEquals(
        expectedWriteModel.getReplaceOptions().isUpsert(),
        writeModel.getReplaceOptions().isUpsert());
  }
}
