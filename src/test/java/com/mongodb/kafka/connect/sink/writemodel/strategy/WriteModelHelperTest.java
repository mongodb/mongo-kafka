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

package com.mongodb.kafka.connect.sink.writemodel.strategy;

import static com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelHelper.flattenKeys;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

@RunWith(JUnitPlatform.class)
class WriteModelHelperTest {

  @Test
  @DisplayName("flatten keys works as expected")
  void testFlattenKeys() {
    assertAll(
        () -> assertEquals(BsonDocument.parse("{}"), flattenKeys(BsonDocument.parse("{}"))),
        () ->
            assertEquals(
                BsonDocument.parse("{a: 1, b: 1, c: [1]}"),
                flattenKeys(BsonDocument.parse("{a: 1, b: 1, c: [1]}"))),
        () ->
            assertEquals(
                BsonDocument.parse(
                    "{'a.a': 1, 'b.b': 1, 'b.c.d': 1, 'b.c.e.f': 1, g: [{h: 1, i: {j: 1}}]}"),
                flattenKeys(
                    BsonDocument.parse(
                        "{a: {a: 1}, b: {b: 1, c: {d: 1, e: {f: 1}}}, g: [{h: 1, i: {j: 1}}]}"))));
  }
}
