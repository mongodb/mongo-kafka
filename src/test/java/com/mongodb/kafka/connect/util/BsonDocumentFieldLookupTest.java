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

package com.mongodb.kafka.connect.util;

import static com.mongodb.kafka.connect.util.BsonDocumentFieldLookup.fieldLookup;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

@RunWith(JUnitPlatform.class)
public class BsonDocumentFieldLookupTest {

  private static final BsonDocument BSON_DOCUMENT =
      BsonDocument.parse(
          "{"
              + "\"a\": 1234,"
              + "\"subDoc\": {"
              + "  \"A\": 123,"
              + "  \"B\": {\"C\": \"12345.6789\"}}"
              + "}");

  @Test
  @DisplayName("Should return the expected value")
  void testFieldLookup() {
    assertAll(
        () -> assertEquals(Optional.of(BSON_DOCUMENT.get("a")), fieldLookup("a", BSON_DOCUMENT)),
        () ->
            assertEquals(
                Optional.of(BSON_DOCUMENT.get("subDoc")), fieldLookup("subDoc", BSON_DOCUMENT)),
        () ->
            assertEquals(
                Optional.of(BSON_DOCUMENT.getDocument("subDoc").get("A")),
                fieldLookup("subDoc.A", BSON_DOCUMENT)),
        () ->
            assertEquals(
                Optional.of(BSON_DOCUMENT.getDocument("subDoc").get("B")),
                fieldLookup("subDoc.B", BSON_DOCUMENT)),
        () ->
            assertEquals(
                Optional.of(BSON_DOCUMENT.getDocument("subDoc").getDocument("B").get("C")),
                fieldLookup("subDoc.B.C", BSON_DOCUMENT)),
        () -> assertEquals(Optional.empty(), fieldLookup("A", BSON_DOCUMENT)),
        () -> assertEquals(Optional.empty(), fieldLookup("A.", BSON_DOCUMENT)),
        () -> assertEquals(Optional.empty(), fieldLookup("subDoc.a", BSON_DOCUMENT)),
        () -> assertEquals(Optional.empty(), fieldLookup("subDoc.a.b", BSON_DOCUMENT)),
        () -> assertEquals(Optional.empty(), fieldLookup("subDoc.B.C.D", BSON_DOCUMENT)));
  }
}
