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

package com.mongodb.kafka.connect.sink.cdc.qlik.rdbms.operations;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.stream.Stream;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.cdc.qlik.OperationType;

@RunWith(JUnitPlatform.class)
class OperationHandlerTest {

  private static final String MESSAGE_TEMPLATE = "{message: {operation: '%s'}}";
  private static final String HEADERS_TEMPLATE = "{headers: {operation: '%s'}}";
  private static final String MESSAGE_HEADERS_TEMPLATE = "{message: {headers: {operation: '%s'}}}";
  private static final String MESSAGE_HEADERS_FLAT_TEMPLATE =
      "{message: {'headers_operation': '%s'}}";
  private static final String INVALID_DOCUMENT = "{a: 1}";
  private static final String INVALID_MESSAGE = "{message: {operation: {a: 1}}}}";
  private static final String INVALID_HEADERS = "{headers: {operation: {a: 1}}}";
  private static final String INVALID_MESSAGE_HEADERS = "{message: {headers: {operation: {a: 1}}}}";
  private static final String INVALID_MESSAGE_HEADERS_FLAT =
      "{message: {headers_operation: {a: 1}}}}";

  @TestFactory
  Stream<DynamicTest> testValidOperations() {
    return Stream.of(
            OperationType.INSERT,
            OperationType.READ,
            OperationType.REFRESH,
            OperationType.DELETE,
            OperationType.UPDATE,
            OperationType.UNKNOWN)
        .map(
            operationType ->
                dynamicTest(
                    format("test %s operations", operationType.getValue()),
                    () ->
                        assertAll(
                            () ->
                                assertEquals(
                                    operationType,
                                    OperationHelper.getOperationType(
                                        BsonDocument.parse(
                                            format(MESSAGE_TEMPLATE, operationType.getValue())))),
                            () ->
                                assertEquals(
                                    operationType,
                                    OperationHelper.getOperationType(
                                        BsonDocument.parse(
                                            format(HEADERS_TEMPLATE, operationType.getValue())))),
                            () ->
                                assertEquals(
                                    operationType,
                                    OperationHelper.getOperationType(
                                        BsonDocument.parse(
                                            format(
                                                MESSAGE_HEADERS_TEMPLATE,
                                                operationType.getValue())))),
                            () ->
                                assertEquals(
                                    operationType,
                                    OperationHelper.getOperationType(
                                        BsonDocument.parse(
                                            format(
                                                MESSAGE_HEADERS_FLAT_TEMPLATE,
                                                operationType.getValue())))))));
  }

  @Test
  @DisplayName("test invalid operation messages")
  void testInvalidOperationMessages() {
    assertAll(
        () ->
            assertThrows(
                DataException.class,
                () -> OperationHelper.getOperationType(BsonDocument.parse(INVALID_DOCUMENT))),
        () ->
            assertThrows(
                DataException.class,
                () -> OperationHelper.getOperationType(BsonDocument.parse(INVALID_HEADERS))),
        () ->
            assertThrows(
                DataException.class,
                () -> OperationHelper.getOperationType(BsonDocument.parse(INVALID_MESSAGE))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    OperationHelper.getOperationType(BsonDocument.parse(INVALID_MESSAGE_HEADERS))),
        () ->
            assertThrows(
                DataException.class,
                () ->
                    OperationHelper.getOperationType(
                        BsonDocument.parse(INVALID_MESSAGE_HEADERS_FLAT))));
  }
}
