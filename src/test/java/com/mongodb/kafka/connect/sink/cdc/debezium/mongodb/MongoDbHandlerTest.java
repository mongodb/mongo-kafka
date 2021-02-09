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
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ErrorTolerance;
import com.mongodb.kafka.connect.sink.cdc.debezium.OperationType;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class MongoDbHandlerTest {

  private static final MongoDbHandler HANDLER_DEFAULT_MAPPING =
      new MongoDbHandler(createTopicConfig());

  private static final MongoDbHandler ERROR_TOLERANT_HANDLER =
      new MongoDbHandler(
          createTopicConfig(
              MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG, ErrorTolerance.ALL.value()));

  @Test
  @DisplayName("verify existing default config from base class")
  void testExistingDefaultConfig() {
    assertAll(
        () ->
            assertNotNull(
                HANDLER_DEFAULT_MAPPING.getConfig(), "default config for handler must not be null"),
        () ->
            assertNotNull(
                new MongoDbHandler(createTopicConfig(), emptyMap()).getConfig(),
                "default config for handler must not be null"));
  }

  @Test
  @DisplayName("when key document missing then DataException")
  void testMissingKeyDocument() {
    assertThrows(
        DataException.class, () -> HANDLER_DEFAULT_MAPPING.handle(new SinkDocument(null, null)));
  }

  @Test
  @DisplayName("when key doc contains 'id' field but value is empty then null due to tombstone")
  void testTombstoneEvent() {
    assertEquals(
        Optional.empty(),
        HANDLER_DEFAULT_MAPPING.handle(
            new SinkDocument(new BsonDocument("id", new BsonInt32(1234)), new BsonDocument())),
        "tombstone event must result in Optional.empty()");
  }

  @Test
  @DisplayName("when value doc contains unknown operation type then DataException")
  void testUnkownCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            new BsonDocument("id", new BsonInt32(1234)),
            new BsonDocument("op", new BsonString("x")));
    assertThrows(DataException.class, () -> HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
    assertEquals(Optional.empty(), ERROR_TOLERANT_HANDLER.handle(cdcEvent));
  }

  @Test
  @DisplayName("when value doc contains unmapped operation type then DataException")
  void testUnmappedCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            new BsonDocument("_id", new BsonInt32(1234)),
            new BsonDocument("op", new BsonString("z"))
                .append("after", new BsonString("{_id:1234,foo:\"blah\"}")));

    assertThrows(DataException.class, () -> HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
    Optional<WriteModel<BsonDocument>> handle = ERROR_TOLERANT_HANDLER.handle(cdcEvent);
    assertEquals(Optional.empty(), handle);
  }

  @Test
  @DisplayName("when value doc contains operation type other than string then DataException")
  void testInvalidCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            new BsonDocument("id", new BsonInt32(1234)),
            new BsonDocument("op", new BsonInt32('c')));

    assertThrows(DataException.class, () -> HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
    assertEquals(Optional.empty(), ERROR_TOLERANT_HANDLER.handle(cdcEvent));
  }

  @Test
  @DisplayName("when value doc is missing operation type then DataException")
  void testMissingCdcOperationType() {
    SinkDocument cdcEvent =
        new SinkDocument(
            new BsonDocument("id", new BsonInt32(1234)), new BsonDocument("po", BsonNull.VALUE));

    assertThrows(DataException.class, () -> HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
    assertEquals(Optional.empty(), ERROR_TOLERANT_HANDLER.handle(cdcEvent));
  }

  @TestFactory
  @DisplayName("when valid CDC event then correct WriteModel")
  Stream<DynamicTest> testValidCdcDocument() {

    return Stream.of(
        dynamicTest(
            "test operation " + OperationType.CREATE,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          new BsonDocument("_id", new BsonString("1234")),
                          new BsonDocument("op", new BsonString("c"))
                              .append("after", new BsonString("{_id:1234,foo:\"blah\"}"))));
              assertTrue(result.isPresent());
              assertTrue(
                  result.get() instanceof ReplaceOneModel,
                  "result expected to be of type ReplaceOneModel");
            }),
        dynamicTest(
            "test operation " + OperationType.READ,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          new BsonDocument("_id", new BsonString("1234")),
                          new BsonDocument("op", new BsonString("r"))
                              .append("after", new BsonString("{_id:1234,foo:\"blah\"}"))));
              assertTrue(result.isPresent());
              assertTrue(
                  result.get() instanceof ReplaceOneModel,
                  "result expected to be of type ReplaceOneModel");
            }),
        dynamicTest(
            "test operation " + OperationType.UPDATE,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          new BsonDocument("id", new BsonString("1234")),
                          new BsonDocument("op", new BsonString("u"))
                              .append("patch", new BsonString("{\"$set\":{foo:\"blah\"}}"))));
              assertTrue(result.isPresent());
              assertTrue(
                  result.get() instanceof UpdateOneModel,
                  "result expected to be of type UpdateOneModel");
            }),
        dynamicTest(
            "test operation " + OperationType.DELETE,
            () -> {
              Optional<WriteModel<BsonDocument>> result =
                  HANDLER_DEFAULT_MAPPING.handle(
                      new SinkDocument(
                          new BsonDocument("id", new BsonString("1234")),
                          new BsonDocument("op", new BsonString("d"))));
              assertTrue(result.isPresent(), "write model result must be present");
              assertTrue(
                  result.get() instanceof DeleteOneModel,
                  "result expected to be of type DeleteOneModel");
            }));
  }

  @TestFactory
  @DisplayName("when valid cdc operation type then correct MongoDB CdcOperation")
  Stream<DynamicTest> testValidCdcOpertionTypes() {

    return Stream.of(
        dynamicTest(
            "test operation " + OperationType.CREATE,
            () ->
                assertTrue(
                    HANDLER_DEFAULT_MAPPING.getCdcOperation(
                            new BsonDocument("op", new BsonString("c")))
                        instanceof MongoDbInsert)),
        dynamicTest(
            "test operation " + OperationType.READ,
            () ->
                assertTrue(
                    HANDLER_DEFAULT_MAPPING.getCdcOperation(
                            new BsonDocument("op", new BsonString("r")))
                        instanceof MongoDbInsert)),
        dynamicTest(
            "test operation " + OperationType.UPDATE,
            () ->
                assertTrue(
                    HANDLER_DEFAULT_MAPPING.getCdcOperation(
                            new BsonDocument("op", new BsonString("u")))
                        instanceof MongoDbUpdate)),
        dynamicTest(
            "test operation " + OperationType.DELETE,
            () ->
                assertTrue(
                    HANDLER_DEFAULT_MAPPING.getCdcOperation(
                            new BsonDocument("op", new BsonString("d")))
                        instanceof MongoDbDelete)));
  }
}
