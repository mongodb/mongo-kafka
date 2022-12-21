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

package com.mongodb.kafka.connect.source.schema;

import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.inferDocumentSchema;
import static java.util.Arrays.asList;
import static org.apache.kafka.connect.data.Schema.Type.ARRAY;
import static org.apache.kafka.connect.data.Schema.Type.INT32;
import static org.apache.kafka.connect.data.Schema.Type.STRING;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;

@SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
public class BsonDocumentToSchemaTest {

  @Test
  void testEmptyArray() {
    BsonDocument document =
        new BsonDocument()
            .append(
                "outerArray",
                new BsonArray(
                    asList(
                        new BsonDocument("innerArray", new BsonArray()),
                        new BsonDocument(
                            "innerArray",
                            new BsonArray(asList(new BsonInt32(1), new BsonInt32(2)))),
                        new BsonDocument(
                            "innerArray",
                            new BsonArray(asList(new BsonInt32(3), new BsonInt32(4)))))));

    Schema schema = inferDocumentSchema(document);
    assertEquals(STRUCT, schema.type());

    Schema outerArraySchema = schema.field("outerArray").schema();
    assertEquals(STRUCT, outerArraySchema.valueSchema().type());

    Schema innerArraySchema = outerArraySchema.valueSchema().field("innerArray").schema();
    assertEquals(ARRAY, innerArraySchema.type());
    assertEquals(INT32, innerArraySchema.valueSchema().type());
  }

  @Test
  void testStructCombining() {
    BsonDocument document =
        new BsonDocument()
            .append(
                "outerArray",
                new BsonArray(
                    asList(
                        new BsonDocument()
                            .append("a", new BsonInt32(1))
                            .append("b", new BsonString("foo")),
                        new BsonDocument()
                            .append("b", new BsonString("foo"))
                            .append("c", new BsonInt32(2)))));

    Schema schema = inferDocumentSchema(document);
    assertEquals(STRUCT, schema.type());

    Schema outerArraySchema = schema.field("outerArray").schema();
    assertEquals(STRUCT, outerArraySchema.valueSchema().type());

    Schema outerArrayValueSchema = outerArraySchema.valueSchema();
    assertEquals(STRUCT, outerArrayValueSchema.type());

    assertEquals(INT32, outerArrayValueSchema.field("a").schema().type());
    assertEquals(STRING, outerArrayValueSchema.field("b").schema().type());
    assertEquals(INT32, outerArrayValueSchema.field("c").schema().type());
  }

  @Test
  void testStructCombiningNestedArrays() {
    BsonDocument document =
        new BsonDocument()
            .append(
                "outerArray",
                new BsonArray(
                    asList(
                        new BsonDocument()
                            .append(
                                "innerArray",
                                new BsonArray(
                                    asList(
                                        new BsonDocument()
                                            .append("a", new BsonInt32(1))
                                            .append("b", new BsonString("foo"))))),
                        new BsonDocument()
                            .append(
                                "innerArray",
                                new BsonArray(
                                    asList(
                                        new BsonDocument()
                                            .append("b", new BsonString("foo"))
                                            .append("c", new BsonInt32(2))))))));

    Schema schema = inferDocumentSchema(document);
    assertEquals(STRUCT, schema.type());

    Schema outerArraySchema = schema.field("outerArray").schema();
    assertEquals(STRUCT, outerArraySchema.valueSchema().type());

    Schema outerArrayValueSchema = outerArraySchema.valueSchema();
    assertEquals(STRUCT, outerArrayValueSchema.type());

    Schema innerArraySchema = outerArrayValueSchema.field("innerArray").schema();
    assertEquals(ARRAY, innerArraySchema.type());

    Schema innerArrayValueSchema = innerArraySchema.valueSchema();
    assertEquals(STRUCT, innerArrayValueSchema.type());

    assertEquals(INT32, innerArrayValueSchema.field("a").schema().type());
    assertEquals(STRING, innerArrayValueSchema.field("b").schema().type());
    assertEquals(INT32, innerArrayValueSchema.field("c").schema().type());
  }

  @Test
  void testStructCombiningWithNulls() {
    BsonDocument document =
        new BsonDocument()
            .append(
                "outerArray",
                new BsonArray(
                    asList(
                        new BsonDocument()
                            .append("a", new BsonInt32(1))
                            .append("b", new BsonString("foo"))
                            .append("c", BsonNull.VALUE)
                            .append("d", BsonNull.VALUE)
                            .append("e", new BsonArray()),
                        new BsonDocument()
                            .append("a", BsonNull.VALUE)
                            .append("b", new BsonString("foo"))
                            .append("c", new BsonInt32(2)))));

    Schema schema = inferDocumentSchema(document);
    assertEquals(STRUCT, schema.type());

    Schema outerArraySchema = schema.field("outerArray").schema();
    assertEquals(STRUCT, outerArraySchema.valueSchema().type());

    Schema outerArrayValueSchema = outerArraySchema.valueSchema();
    assertEquals(STRUCT, outerArrayValueSchema.type());

    assertEquals(INT32, outerArrayValueSchema.field("a").schema().type());
    assertEquals(STRING, outerArrayValueSchema.field("b").schema().type());
    assertEquals(INT32, outerArrayValueSchema.field("c").schema().type());
    assertEquals(STRING, outerArrayValueSchema.field("d").schema().type());
    assertNull(outerArrayValueSchema.field("d").schema().defaultValue());
    assertEquals(STRING, outerArrayValueSchema.field("e").schema().valueSchema().type());
    assertNull(outerArrayValueSchema.field("e").schema().valueSchema().defaultValue());
  }

  @Test
  void testUncombinableDocumentArrayElements() {
    BsonDocument document =
        new BsonDocument()
            .append(
                "outerArray",
                new BsonArray(
                    asList(
                        new BsonDocument("a", new BsonArray(asList(new BsonInt32(1)))),
                        new BsonDocument("a", new BsonArray(asList(new BsonString("foo")))))));

    Schema schema = inferDocumentSchema(document);
    assertEquals(STRUCT, schema.type());

    Schema outerArraySchema = schema.field("outerArray").schema();
    assertEquals(STRING, outerArraySchema.valueSchema().type());
  }

  @Test
  void testUncombinableArrayArrayElements() {
    BsonDocument document =
        new BsonDocument()
            .append(
                "outerArray",
                new BsonArray(
                    asList(
                        new BsonArray(asList(new BsonInt32(1))),
                        new BsonArray(asList(new BsonString("foo"))))));

    Schema schema = inferDocumentSchema(document);
    assertEquals(STRUCT, schema.type());

    Schema outerArraySchema = schema.field("outerArray").schema();
    assertEquals(STRING, outerArraySchema.valueSchema().type());
  }
}
