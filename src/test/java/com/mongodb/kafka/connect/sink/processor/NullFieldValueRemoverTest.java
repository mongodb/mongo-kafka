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

package com.mongodb.kafka.connect.sink.processor;

import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class NullFieldValueRemoverTest {
  @Test
  @DisplayName("test NullFieldValueRemoverTest flat document")
  void testNullFieldValueRemoverFlatDocument() {
    BsonDocument document =
        BsonDocument.parse(
            "{"
                + "  'myNull1': null,"
                + "  'myString': 'a',"
                + "  'myEmptyString': ''"
                + "  'myNull2': null,"
                + "  'myTrueBool': true,"
                + "  'myFalseBool': false,"
                + "  'myInt': 123,"
                + "  'myNull3': null"
                + "}");
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, document);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    BsonDocument expected =
        BsonDocument.parse(
            "{"
                + "  'myString': 'a',"
                + "  'myEmptyString': '',"
                + "  'myTrueBool': true,"
                + "  'myFalseBool': false,"
                + "  'myInt': 123"
                + "}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }

  @Test
  @DisplayName("test NullFieldValueRemoverTest document with only null values")
  void testNullFieldValueRemoverObjectWithOnlyNullValues() {
    BsonDocument document =
        BsonDocument.parse("{ 'myNull1': null, 'myNull2': null, 'myNull3': null }");
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, document);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    BsonDocument expected = BsonDocument.parse("{}");
    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }

  @Test
  @DisplayName("test NullFieldValueRemoverTest empty document")
  void testNullFieldValueRemoverEmptyDocument() {
    BsonDocument empty = new BsonDocument();
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, empty);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    BsonDocument expected = BsonDocument.parse("{}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }

  @Test
  @DisplayName("test NullFieldValueRemoverTest nested document")
  void testNullFieldValueRemoverNestedDocument() {
    BsonDocument document =
        BsonDocument.parse(
            "{"
                + "  'myNull1': null,"
                + "  'mySubDoc1': {"
                + "    'myDocumentString': 'a',"
                + "    'myDocumentNull': null,"
                + "    'mySubDoc2': {"
                + "      'myDocumentString': 'b',"
                + "      'myDocumentNull': null,"
                + "      'mySubDoc3': {"
                + "        'myDocumentString': 'c',"
                + "        'myDocumentNull': null"
                + "      }"
                + "    }"
                + "  },"
                + "  'myArray': [null, 'a', { 'a': null, 'b': null, 'c': null }, 123],"
                + "  'myNull2': null,"
                + "  'myDocument': { 'myDocumentString': 'a', 'myDocumentNull': null }"
                + "}");
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, document);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    BsonDocument expected =
        BsonDocument.parse(
            "{"
                + "  'mySubDoc1': {"
                + "    'myDocumentString': 'a',"
                + "    'mySubDoc2': {"
                + "      'myDocumentString': 'b',"
                + "      'mySubDoc3': {"
                + "        'myDocumentString': 'c'"
                + "      }"
                + "    }"
                + "  }, "
                + "  'myArray': [null, 'a', {}, 123],"
                + "  'myDocument': {"
                + "    'myDocumentString': 'a'"
                + "  }"
                + "}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }
}
