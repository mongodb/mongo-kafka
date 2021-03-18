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

package com.mongodb.kafka.connect.sink.cdc.qlik.rdbms;

import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.cdc.qlik.OperationType;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class RdbmsHandlerTest {
  private static final RdbmsHandler RDBMS_HANDLER_DEFAULT_MAPPING =
      new RdbmsHandler(createTopicConfig());
  private static final RdbmsHandler RDBMS_HANDLER_EMPTY_MAPPING =
      new RdbmsHandler(createTopicConfig(), emptyMap());

  @Test
  @DisplayName("verify existing default config from base class")
  void testExistingDefaultConfig() {
    assertAll(
        () ->
            assertNotNull(
                RDBMS_HANDLER_DEFAULT_MAPPING.getConfig(),
                "default config for handler must not be null"),
        () ->
            assertNotNull(
                RDBMS_HANDLER_EMPTY_MAPPING.getConfig(),
                "default config for handler must not be null"));
  }

  @Test
  @DisplayName("when the value is empty then null due to tombstone")
  void testTombstoneEvents() {
    assertEquals(
        Optional.empty(),
        RDBMS_HANDLER_DEFAULT_MAPPING.handle(
            new SinkDocument(BsonDocument.parse("{id: 1234}"), new BsonDocument())),
        "tombstone event must result in Optional.empty()");
    assertEquals(
        Optional.empty(),
        RDBMS_HANDLER_DEFAULT_MAPPING.handle(
            new SinkDocument(new BsonDocument(), new BsonDocument())),
        "tombstone event must result in Optional.empty()");
  }

  @Test
  @DisplayName("when value doc contains unknown operation type then DataException")
  void testUnknownCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            BsonDocument.parse("{id: 1234}"),
            BsonDocument.parse("{message: { headers: { operation: 'x' } } }"));
    assertThrows(DataException.class, () -> RDBMS_HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
  }

  @Test
  @DisplayName("when value doc contains unmapped operation type then DataException")
  void testUnmappedCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            BsonDocument.parse("{id: 1234}"),
            BsonDocument.parse(
                "{message: { headers: { operation: 'CREATE' } , data: {id: 1234, foo: 'bar'}}}"));
    assertThrows(DataException.class, () -> RDBMS_HANDLER_EMPTY_MAPPING.handle(cdcEvent));
  }

  @Test
  @DisplayName("when value doc contains operation type other than string then DataException")
  void testInvalidCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            BsonDocument.parse("{id: 1234}"),
            BsonDocument.parse("{message: { headers: { operation: 5} } }"));
    assertThrows(DataException.class, () -> RDBMS_HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
  }

  @Test
  @DisplayName("when value doc is missing operation type then DataException")
  void testMissingCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            BsonDocument.parse("{id: 1234}"),
            BsonDocument.parse("{message: { headers: { noOperation: 'INSERT'} } }"));
    assertThrows(DataException.class, () -> RDBMS_HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
  }

  @Test
  @DisplayName("when no updates then empty")
  void testNoUpdateOP() {
    assertEquals(
        Optional.empty(),
        RDBMS_HANDLER_DEFAULT_MAPPING.handle(
            new SinkDocument(
                BsonDocument.parse("{id: 1234}"),
                BsonDocument.parse(
                    "{message : { data: {id: 1234, foo: 'bar'}, "
                        + "beforeData: {id: 1234, foo: 'bar'}, headers: { operation: 'UPDATE'}}}"))),
        "No-op update must result in Optional.empty()");
  }

  @TestFactory
  @DisplayName("when valid CDC event then correct WriteModel")
  Stream<DynamicTest> testValidCdcDocument() {

    return Stream.of(
        dynamicTest(
            "test operation " + OperationType.INSERT,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          BsonDocument.parse("{id: 1234}"),
                          BsonDocument.parse(
                              "{message: { data: {id: 1234, foo: 'bar'}, "
                                  + "headers: { operation: 'INSERT'}}}")));
              assertTrue(result.isPresent());
              assertTrue(
                  result.get() instanceof ReplaceOneModel,
                  "result expected to be of type ReplaceOneModel");
            }),
        dynamicTest(
            "test operation " + OperationType.READ,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          BsonDocument.parse("{id: 1234}"),
                          BsonDocument.parse(
                              "{message : { data: {id: 1234, foo: 'bar'}, "
                                  + "headers_operation: 'READ'}}")));
              assertTrue(result.isPresent());
              assertTrue(
                  result.get() instanceof ReplaceOneModel,
                  "result expected to be of type ReplaceOneModel");
            }),
        dynamicTest(
            "test operation " + OperationType.UPDATE,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          BsonDocument.parse("{id: 1234}"),
                          BsonDocument.parse(
                              "{message : { data: {id: 1234, foo: 'bar'}, "
                                  + "beforeData: {id: 4321, foo: 'foo'}, operation: 'UPDATE'}}")));
              assertTrue(result.isPresent());
              assertTrue(
                  result.get() instanceof UpdateOneModel,
                  "result expected to be of type ReplaceOneModel");
            }),
        dynamicTest(
            "test operation " + OperationType.DELETE,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          BsonDocument.parse("{id: 1234}"),
                          BsonDocument.parse(
                              "{ data: {id: 1234, foo: 'bar'}, "
                                  + "headers: { operation: 'DELETE'}}")));
              assertTrue(result.isPresent(), "write model result must be present");
              assertTrue(
                  result.get() instanceof DeleteOneModel,
                  "result expected to be of type DeleteOneModel");
            }));
  }
}
