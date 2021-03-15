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

package com.mongodb.kafka.connect.sink.cdc.mongodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.stream.Stream;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
class OperationTypeTest {

  @TestFactory
  Stream<DynamicTest> dynamicTestsFromCollection() {
    return Stream.of(
        dynamicTest(
            "insert",
            () -> {
              String value = "insert";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.INSERT, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "replace",
            () -> {
              String value = "replace";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.REPLACE, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "update",
            () -> {
              String value = "update";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.UPDATE, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "delete",
            () -> {
              String value = "delete";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.DELETE, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "drop",
            () -> {
              String value = "drop";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.DROP_COLLECTION, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "dropDatabase",
            () -> {
              String value = "dropDatabase";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.DROP_DATABASE, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "rename",
            () -> {
              String value = "rename";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.RENAME_COLLECTION, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "invalidate",
            () -> {
              String value = "invalidate";
              OperationType operationType = OperationType.fromString(value);
              assertAll(
                  () -> assertEquals(OperationType.INVALIDATE, operationType),
                  () -> assertEquals(value, operationType.getValue()));
            }),
        dynamicTest(
            "unknown",
            () -> {
              OperationType operationType = OperationType.fromString("madeUpOperation");
              assertAll(
                  () -> assertEquals(OperationType.UNKNOWN, operationType),
                  () -> assertEquals("unknown", operationType.getValue()));
            }));
  }
}
