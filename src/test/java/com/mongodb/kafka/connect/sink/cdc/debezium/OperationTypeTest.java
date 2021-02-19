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

package com.mongodb.kafka.connect.sink.cdc.debezium;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
class OperationTypeTest {

  @Test
  @DisplayName("when op type 'c' then type CREATE")
  void testOperationTypeCreate() {
    String textType = "c";
    OperationType otCreate = OperationType.fromText(textType);
    assertAll(
        () -> assertEquals(OperationType.CREATE, otCreate),
        () -> assertEquals(textType, otCreate.type()));
  }

  @Test
  @DisplayName("when op type 'r' then type READ")
  void testOperationTypeRead() {
    String textType = "r";
    OperationType otRead = OperationType.fromText(textType);
    assertAll(
        () -> assertEquals(OperationType.READ, otRead),
        () -> assertEquals(textType, otRead.type()));
  }

  @Test
  @DisplayName("when op type 'u' then type UPDATE")
  void testOperationTypeUpdate() {
    String textType = "u";
    OperationType otUpdate = OperationType.fromText(textType);
    assertAll(
        () -> assertEquals(OperationType.UPDATE, otUpdate),
        () -> assertEquals(textType, otUpdate.type()));
  }

  @Test
  @DisplayName("when op type 'd' then type DELETE")
  void testOperationTypeDelete() {
    String textType = "d";
    OperationType otDelete = OperationType.fromText(textType);
    assertAll(
        () -> assertEquals(OperationType.DELETE, otDelete),
        () -> assertEquals(textType, otDelete.type()));
  }

  @Test
  @DisplayName("when invalid op type IllegalArgumentException")
  void testOperationTypeInvalid() {
    assertThrows(DataException.class, () -> OperationType.fromText("x"));
  }
}
