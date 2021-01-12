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

package com.mongodb.kafka.connect.sink.converter;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;

import com.mongodb.kafka.connect.sink.converter.LazyBsonDocument.Type;

@SuppressWarnings({"ResultOfMethodCallIgnored", "MismatchedQueryAndUpdateOfCollection"})
@RunWith(JUnitPlatform.class)
public class LazyBsonDocumentTest {

  private static final String JSON = "{_id: 12345, a: 'a', b: 'b', c: 'c'}";
  private static final String NOT_JSON = "A normal string";
  private static final RawBsonDocument EXPECTED_BSON_DOCUMENT = RawBsonDocument.parse(JSON);
  private static final SinkRecord SINK_RECORD =
      new SinkRecord("topic", 0, Schema.STRING_SCHEMA, JSON, Schema.STRING_SCHEMA, NOT_JSON, 1L);
  private static final SinkRecord SINK_RECORD_ALT_KEY =
      new SinkRecord("topic", 0, Schema.STRING_SCHEMA, NOT_JSON, Schema.STRING_SCHEMA, JSON, 1L);
  private static final SinkRecord SINK_RECORD_ALT_VALUE =
      new SinkRecord("topic", 0, Schema.STRING_SCHEMA, JSON, Schema.STRING_SCHEMA, NOT_JSON, 1L);

  @Test
  @DisplayName("test invalid LazyBsonDocuments")
  void testInvalidLazyBsonDocuments() {
    assertAll(
        "Ensure not nulls",
        () ->
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    new LazyBsonDocument(
                        null, Type.KEY, (Schema schema, Object data) -> new BsonDocument())),
        () ->
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    new LazyBsonDocument(
                        SINK_RECORD, null, (Schema schema, Object data) -> new BsonDocument())),
        () ->
            assertThrows(
                IllegalArgumentException.class,
                () -> new LazyBsonDocument(SINK_RECORD, Type.KEY, null)));
  }

  @Test
  @DisplayName("test invalid conversions")
  void testInvalidConversions() {
    assertAll(
        "Ensure throws DataException when failing to convert",
        () ->
            assertDoesNotThrow(
                () ->
                    new LazyBsonDocument(
                        SINK_RECORD_ALT_KEY,
                        Type.KEY,
                        (Schema schema, Object data) -> BsonDocument.parse(data.toString()))),
        () ->
            assertThrows(
                DataException.class,
                new LazyBsonDocument(
                        SINK_RECORD_ALT_KEY,
                        Type.KEY,
                        (Schema schema, Object data) -> BsonDocument.parse(data.toString()))
                    ::toString),
        () ->
            assertThrows(
                DataException.class,
                new LazyBsonDocument(
                        SINK_RECORD_ALT_VALUE,
                        Type.VALUE,
                        (Schema schema, Object data) -> BsonDocument.parse(data.toString()))
                    ::toString));
  }

  @Test
  @DisplayName("test BsonDocument read API")
  void testBsonDocumentReadAPI() {
    LazyBsonDocument lazyBsonDocument =
        new LazyBsonDocument(
            SINK_RECORD,
            Type.KEY,
            (Schema schema, Object data) -> BsonDocument.parse(data.toString()));
    assertAll(
        "The BsonDocument API returns results as expected",
        () -> assertEquals(EXPECTED_BSON_DOCUMENT.size(), lazyBsonDocument.size()),
        () -> assertEquals(EXPECTED_BSON_DOCUMENT.entrySet(), lazyBsonDocument.entrySet()),
        () -> assertEquals(EXPECTED_BSON_DOCUMENT.keySet(), lazyBsonDocument.keySet()),
        () -> assertIterableEquals(EXPECTED_BSON_DOCUMENT.values(), lazyBsonDocument.values()),
        () ->
            assertEquals(
                EXPECTED_BSON_DOCUMENT.containsValue(new BsonString("a")),
                lazyBsonDocument.containsValue(new BsonString("a"))),
        () ->
            assertEquals(
                EXPECTED_BSON_DOCUMENT.containsKey("a"), lazyBsonDocument.containsKey("a")));
  }

  @Test
  @DisplayName("test clone")
  void testClone() {
    assertAll(
        "The BsonDocument API returns results as expected",
        () -> {
          LazyBsonDocument lazyBsonDocument =
              new LazyBsonDocument(
                  SINK_RECORD,
                  Type.KEY,
                  (Schema schema, Object data) -> BsonDocument.parse(data.toString()));
          assertEquals(lazyBsonDocument, lazyBsonDocument.clone());
        },
        () -> {
          LazyBsonDocument lazyBsonDocument =
              new LazyBsonDocument(
                  SINK_RECORD_ALT_KEY,
                  Type.KEY,
                  (Schema schema, Object data) -> BsonDocument.parse(data.toString()));
          assertDoesNotThrow(lazyBsonDocument::clone);
        },
        () -> {
          LazyBsonDocument lazyBsonDocument =
              new LazyBsonDocument(
                  SINK_RECORD_ALT_VALUE,
                  Type.VALUE,
                  (Schema schema, Object data) -> BsonDocument.parse(data.toString()));
          assertDoesNotThrow(lazyBsonDocument::clone);
        });
  }
}
