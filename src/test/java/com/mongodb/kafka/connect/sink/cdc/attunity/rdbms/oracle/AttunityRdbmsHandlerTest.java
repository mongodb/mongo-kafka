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

package com.mongodb.kafka.connect.sink.cdc.attunity.rdbms.oracle;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.kafka.connect.sink.cdc.debezium.OperationType;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.errors.DataException;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.stream.Stream;

import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
class AttunityRdbmsHandlerTest {
    private static final AttunityRdbmsHandler RDBMS_HANDLER_DEFAULT_MAPPING =
            new AttunityRdbmsHandler(createTopicConfig());
    private static final AttunityRdbmsHandler RDBMS_HANDLER_EMPTY_MAPPING =
            new AttunityRdbmsHandler(createTopicConfig(), emptyMap());

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
    @DisplayName("when key doc contains fields but value is empty then null due to tombstone")
    void testTombstoneEvent1() {
        assertEquals(
                Optional.empty(),
                RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                        new SinkDocument(BsonDocument.parse("{id: 1234}"), new BsonDocument())),
                "tombstone event must result in Optional.empty()");
    }

    @Test
    @DisplayName("when both key doc and value value doc are empty then null due to tombstone")
    void testTombstoneEvent2() {
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
    @DisplayName("when value doc is null operation type then DataException")
    void testNullCdcOperationType() {
        SinkDocument cdcEvent =
                new SinkDocument(
                        BsonDocument.parse("{id: 1234}"),
                        BsonDocument.parse("{message: { headers: { operation: null} } }"));
        assertThrows(DataException.class, () -> RDBMS_HANDLER_DEFAULT_MAPPING.handle(cdcEvent));
    }

    @Test
    @DisplayName("when value doc is missing operation type then DataException")
    void testMissingCdcOperationType() {
        SinkDocument cdcEvent =
                new SinkDocument(
                        BsonDocument.parse("{id: 1234}"),
                        BsonDocument.parse("{message: { headers: { noperation: 'CREATE'} } }"));
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
                        "test operation " + OperationType.CREATE,
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
                                                                    + "headers: { operation: 'READ'}}}")));
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
                                                                    + "beforeData: {id: 4321, foo: 'foo'}, headers: { operation: 'UPDATE'}}}")));
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
                                                            "{message : { data: {id: 1234, foo: 'bar'}, "
                                                                    + "headers: { operation: 'DELETE'}}}")));
                            assertTrue(result.isPresent(), "write model result must be present");
                            assertTrue(
                                    result.get() instanceof DeleteOneModel,
                                    "result expected to be of type DeleteOneModel");
                        }));
    }

    @TestFactory
    @DisplayName("when valid CDC event then correct WriteModel with Avro")
    Stream<DynamicTest> testValidCdcDocumentWithoutWrapper() {

        return Stream.of(
                dynamicTest(
                        "test operation " + OperationType.CREATE,
                        () -> {
                            Optional<WriteModel<BsonDocument>> result =
                                    RDBMS_HANDLER_DEFAULT_MAPPING.handle(
                                            new SinkDocument(
                                                    BsonDocument.parse("{id: 1234}"),
                                                    BsonDocument.parse(
                                                            "{data: {id: 1234, foo: 'bar'}, "
                                                                    + "headers: { operation: 'INSERT'}}")));
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
                                                            "{ data: {id: 1234, foo: 'bar'}, "
                                                                    + "headers: { operation: 'READ'}}")));
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
                                                            "{data: {id: 1234, foo: 'bar'}, "
                                                                    + "beforeData: {id: 4321, foo: 'foo'}, headers: { operation: 'UPDATE'}}")));
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
                                                            "{data: {id: 1234, foo: 'bar'}, "
                                                                    + "headers: { operation: 'DELETE'}}")));
                            assertTrue(result.isPresent(), "write model result must be present");
                            assertTrue(
                                    result.get() instanceof DeleteOneModel,
                                    "result expected to be of type DeleteOneModel");
                        }));
    }

    @TestFactory
    @DisplayName("when valid cdc operation type then correct RDBMS CdcOperation")
    Stream<DynamicTest> testValidCdcOperationTypes() {
        return Stream.of(
                dynamicTest(
                        "test operation " + OperationType.CREATE,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{message: { headers: { operation: 'INSERT'} } }"))
                                                instanceof AttunityRdbmsInsert)),
                dynamicTest(
                        "test operation " + OperationType.READ,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{message: { headers: { operation: 'READ'} } }"))
                                                instanceof AttunityRdbmsInsert)),
                dynamicTest(
                        "test operation " + OperationType.UPDATE,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{message: { headers: { operation: 'UPDATE'} } }"))
                                                instanceof AttunityRdbmsUpdate)),
                dynamicTest(
                        "test operation " + OperationType.DELETE,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{message: { headers: { operation: 'DELETE'} } }"))
                                                instanceof AttunityRdbmsDelete)));
    }

    @TestFactory
    @DisplayName("when valid cdc operation type then correct RDBMS CdcOperation without Qlik Replicate wrapper")
    Stream<DynamicTest> testValidOperationTypesWithoutWrapper() {
        return Stream.of(
                dynamicTest(
                        "test operation " + OperationType.CREATE,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{ headers: { operation: 'INSERT'} }"))
                                                instanceof AttunityRdbmsInsert)),
                dynamicTest(
                        "test operation " + OperationType.READ,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{ headers: { operation: 'READ'} }"))
                                                instanceof AttunityRdbmsInsert)),
                dynamicTest(
                        "test operation " + OperationType.UPDATE,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{ headers: { operation: 'UPDATE'} }"))
                                                instanceof AttunityRdbmsUpdate)),
                dynamicTest(
                        "test operation " + OperationType.DELETE,
                        () ->
                                assertTrue(
                                        RDBMS_HANDLER_DEFAULT_MAPPING.getCdcOperation(
                                                BsonDocument.parse("{ headers: { operation: 'DELETE'} }"))
                                                instanceof AttunityRdbmsDelete)));
    }
}
