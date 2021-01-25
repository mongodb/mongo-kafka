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

package com.mongodb.kafka.connect.sink.namespace.mapping;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.util.ConnectConfigException;

@RunWith(JUnitPlatform.class)
public class FieldPathNamespaceMapperTest {

  private static final SinkRecord SINK_RECORD =
      new SinkRecord(
          TEST_TOPIC,
          0,
          Schema.STRING_SCHEMA,
          "{'db': 'dbKey', 'coll': 'collKey', 'invalid': 1, 'ns': {'db': 'nsDbKey', 'coll': 'nsCollKey'}}",
          Schema.STRING_SCHEMA,
          "{'db': 'dbValue', 'coll': 'collValue', 'invalid': 1, 'ns': {'db': 'nsDbValue', 'coll': 'nsCollValue'}}",
          1);
  private static final SinkDocument SINK_DOCUMENT =
      new SinkDocument(
          BsonDocument.parse(SINK_RECORD.key().toString()),
          BsonDocument.parse(SINK_RECORD.value().toString()));

  @Test
  @DisplayName("test produces the expected namespace")
  void testProducesTheExpectedNamespace() {
    assertAll(
        () ->
            assertEquals(
                "dbKey.topic",
                createMapper(
                        createTopicConfig(
                            format("{'%s': 'db'}", FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "myDB.collKey",
                createMapper(
                        createTopicConfig(
                            format("{'%s': 'coll'}", FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "dbKey.collKey",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'db', '%s': 'coll'}",
                                FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "nsDbKey.nsCollKey",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'ns.db', '%s': 'ns.coll'}",
                                FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "dbValue.topic",
                createMapper(
                        createTopicConfig(
                            format("{'%s': 'db'}", FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "myDB.collValue",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'coll'}", FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "dbValue.collValue",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'db', '%s': 'coll'}",
                                FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "nsDbValue.nsCollValue",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'ns.db', '%s': 'ns.coll'}",
                                FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "dbKey.collValue",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'db', '%s': 'coll'}",
                                FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "dbValue.collKey",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'db', '%s': 'coll'}",
                                FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "myDB.collValue",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'missing', '%s': 'coll'}",
                                FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "dbValue.topic",
                createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'db', '%s': 'missing'}",
                                FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG)))
                    .getNamespace(SINK_RECORD, SINK_DOCUMENT)
                    .getFullName()));
  }

  @Test
  @DisplayName("test validates the configuration")
  void testValidatesTheConfiguration() {
    assertAll(
        "Ensure the configuration is validated",
        () -> assertThrows(ConnectConfigException.class, () -> createMapper(createTopicConfig())),
        () ->
            assertThrows(
                ConnectConfigException.class,
                () ->
                    createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'keyPath', '%s': 'valuePath'}",
                                FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
                                FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG)))),
        () ->
            assertThrows(
                ConnectConfigException.class,
                () ->
                    createMapper(
                        createTopicConfig(
                            format(
                                "{'%s': 'keyPath', '%s': 'valuePath'}",
                                FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG,
                                FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG)))));
  }

  @TestFactory
  @DisplayName("test handles invalid or missing data")
  Stream<DynamicTest> testHandlesInvalidOrMissingData() {
    List<String> configs =
        asList(
            FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
            FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG,
            FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG,
            FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG);

    List<DynamicTest> dynamicTests = new ArrayList<>();
    configs.forEach(
        configName -> {
          dynamicTests.add(
              DynamicTest.dynamicTest(
                  "test handles invalid data : " + configName,
                  () -> {
                    DataException e =
                        assertThrows(
                            DataException.class,
                            () ->
                                createMapper(
                                        createTopicConfig(
                                            format(
                                                "{'%s': 'invalid', '%s': true}",
                                                configName,
                                                FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG)))
                                    .getNamespace(SINK_RECORD, SINK_DOCUMENT));
                    assertTrue(
                        e.getMessage()
                            .startsWith(
                                "Invalid type for INT32 field path 'invalid', expected a String"));
                  }));

          dynamicTests.add(
              DynamicTest.dynamicTest(
                  "test handles missing data : " + configName,
                  () -> {
                    DataException e =
                        assertThrows(
                            DataException.class,
                            () ->
                                createMapper(
                                        createTopicConfig(
                                            format(
                                                "{'%s': 'missing', '%s': true}",
                                                configName,
                                                FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG)))
                                    .getNamespace(SINK_RECORD, SINK_DOCUMENT));

                    assertTrue(
                        e.getMessage().startsWith("Missing document path 'missing'"),
                        e.getMessage());
                  }));
        });
    return dynamicTests.stream();
  }

  private NamespaceMapper createMapper(final MongoSinkTopicConfig config) {
    NamespaceMapper namespaceMapper = new FieldPathNamespaceMapper();
    namespaceMapper.configure(config);
    return namespaceMapper;
  }
}
