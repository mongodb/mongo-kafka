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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class NullFieldValueRemoverTest {
  @Test
  @DisplayName("test NullFieldValueRemoverTest flat document")
  void testNullFieldValueRemoverFlatDocument() {
    List<BsonElement> elements =
        Arrays.asList(
            new BsonElement("myNull1", new BsonNull()),
            new BsonElement("myString", new BsonString("a")),
            new BsonElement("myEmptyString", new BsonString("")),
            new BsonElement("myNull2", new BsonNull()),
            new BsonElement("myTrueBool", new BsonBoolean(true)),
            new BsonElement("myFalseBool", new BsonBoolean(false)),
            new BsonElement("myInt", new BsonInt32(123)),
            new BsonElement("myNull3", new BsonNull()));
    BsonDocument containsNullFieldValues = new BsonDocument(elements);
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, containsNullFieldValues);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    BsonDocument expected =
        BsonDocument.parse(
            "{'myString': 'a', 'myEmptyString': '', 'myTrueBool': true, 'myFalseBool': false, 'myInt': 123}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());

    elements =
        Arrays.asList(
            new BsonElement("myNull1", new BsonNull()),
            new BsonElement("myNull2", new BsonNull()),
            new BsonElement("myNull3", new BsonNull()));
    BsonDocument containsOnlyNullFieldValues = new BsonDocument(elements);
    sinkDocWithValueDoc = new SinkDocument(null, containsOnlyNullFieldValues);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    expected = BsonDocument.parse("{}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());

    BsonDocument empty = new BsonDocument();
    sinkDocWithValueDoc = new SinkDocument(null, empty);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    expected = BsonDocument.parse("{}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }

  @Test
  @DisplayName("test NullFieldValueRemoverTest nested document")
  void testNullFieldValueRemoverNestedDocument() {
    List<BsonElement> elements =
        Arrays.asList(
            new BsonElement("myNull1", new BsonNull()),
            new BsonElement(
                "mySubDoc1",
                new BsonDocument(
                    Arrays.asList(
                        new BsonElement("myDocumentString", new BsonString("a")),
                        new BsonElement("myDocumentNull", new BsonNull()),
                        new BsonElement(
                            "mySubDoc2",
                            new BsonDocument(
                                Arrays.asList(
                                    new BsonElement("myDocumentString", new BsonString("b")),
                                    new BsonElement("myDocumentNull", new BsonNull()),
                                    new BsonElement(
                                        "mySubDoc3",
                                        new BsonDocument(
                                            Arrays.asList(
                                                new BsonElement(
                                                    "myDocumentString", new BsonString("c")),
                                                new BsonElement(
                                                    "myDocumentNull", new BsonNull())))))))))),
            new BsonElement(
                "myArray",
                new BsonArray(
                    Arrays.asList(
                        new BsonNull(),
                        new BsonString("a"),
                        new BsonDocument(
                            Arrays.asList(
                                new BsonElement("myArrayValueDocumentNull1", new BsonNull()),
                                new BsonElement("myArrayValueDocumentNull2", new BsonNull()),
                                new BsonElement("myArrayValueDocumentNull3", new BsonNull()))),
                        new BsonInt32(123)))),
            new BsonElement("myNull3", new BsonNull()),
            new BsonElement(
                "myDocument",
                new BsonDocument(
                    Arrays.asList(
                        new BsonElement("myDocumentString", new BsonString("a")),
                        new BsonElement("myDocumentNull", new BsonNull())))),
            new BsonElement(
                "myDocumentAllNullFields",
                new BsonDocument(
                    Arrays.asList(
                        new BsonElement("myDocumentNull1", new BsonNull()),
                        new BsonElement("myDocumentNull2", new BsonNull()),
                        new BsonElement("myDocumentNull3", new BsonNull())))));
    BsonDocument containsNullFieldValues = new BsonDocument(elements);
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, containsNullFieldValues);
    new NullFieldValueRemover(createTopicConfig()).process(sinkDocWithValueDoc, null);
    BsonDocument expected =
        BsonDocument.parse(
            "{'mySubDoc1': {'myDocumentString': 'a', 'mySubDoc2': {'myDocumentString': 'b', "
                + "'mySubDoc3': {'myDocumentString': 'c'}}}, 'myArray': [null, 'a', {}, 123], "
                + "'myDocument': {'myDocumentString': 'a'}, 'myDocumentAllNullFields': {}}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }
}
