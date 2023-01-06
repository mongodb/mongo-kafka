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

import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.DEFAULT_FIELD_NAME;
import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.INCOMPATIBLE_SCHEMA_TYPE;
import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.SENTINEL_STRING_TYPE;
import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.inferDocumentSchema;
import static com.mongodb.kafka.connect.source.schema.BsonDocumentToSchema.isSentinel;
import static com.mongodb.kafka.connect.source.schema.SchemaUtils.assertSchemaEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

public class BsonDocumentToSchemaTest {

  @Test
  void testInferringAllBsonTypes() {
    BsonDocument bsonDocument =
        BsonDocument.parse(
            "{"
                + "\"array\": [{\"$numberInt\": \"1\"}, {\"$numberInt\": \"2\"}, {\"$numberInt\": \"3\"}], "
                + "\"binary\": {\"$binary\": {\"base64\": \"S2Fma2Egcm9ja3Mh\", \"subType\": \"00\"}}, "
                + "\"boolean\": true, "
                + "\"code\": {\"$code\": \"int i = 0;\"}, "
                + "\"codeWithScope\": {\"$code\": \"int x = y\", \"$scope\": {\"y\": {\"$numberInt\": \"1\"}}}, "
                + "\"dateTime\": {\"$date\": {\"$numberLong\": \"1577836801000\"}}, "
                + "\"decimal128\": {\"$numberDecimal\": \"1.0\"}, "
                + "\"document\": {\"a\": {\"$numberInt\": \"1\"}}, "
                + "\"double\": {\"$numberDouble\": \"62.0\"}, "
                + "\"int32\": {\"$numberInt\": \"42\"}, "
                + "\"int64\": {\"$numberLong\": \"52\"}, "
                + "\"maxKey\": {\"$maxKey\": 1}, "
                + "\"minKey\": {\"$minKey\": 1}, "
                + "\"null\": null, "
                + "\"objectId\": {\"$oid\": \"5f3d1bbde0ca4d2829c91e1d\"}, "
                + "\"regex\": {\"$regularExpression\": {\"pattern\": \"^test.*regex.*xyz$\", \"options\": \"i\"}}, "
                + "\"string\": \"the fox ...\", "
                + "\"symbol\": {\"$symbol\": \"ruby stuff\"}, "
                + "\"timestamp\": {\"$timestamp\": {\"t\": 305419896, \"i\": 5}}, "
                + "\"undefined\": {\"$undefined\": true}"
                + "}");

    Schema expected =
        SchemaBuilder.struct()
            .name(DEFAULT_FIELD_NAME)
            .field("array", createArray("array", Schema.OPTIONAL_INT32_SCHEMA))
            .field("binary", Schema.OPTIONAL_BYTES_SCHEMA)
            .field("boolean", Schema.OPTIONAL_BOOLEAN_SCHEMA)
            .field("code", Schema.OPTIONAL_STRING_SCHEMA)
            .field("codeWithScope", Schema.OPTIONAL_STRING_SCHEMA)
            .field("dateTime", Timestamp.builder().optional().build())
            .field("decimal128", Decimal.builder(1).optional().build())
            .field(
                "document",
                SchemaBuilder.struct()
                    .field("a", Schema.OPTIONAL_INT32_SCHEMA)
                    .name("document")
                    .optional()
                    .build())
            .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
            .field("int32", Schema.OPTIONAL_INT32_SCHEMA)
            .field("int64", Schema.OPTIONAL_INT64_SCHEMA)
            .field("maxKey", Schema.OPTIONAL_STRING_SCHEMA)
            .field("minKey", Schema.OPTIONAL_STRING_SCHEMA)
            .field("null", Schema.OPTIONAL_STRING_SCHEMA)
            .field("objectId", Schema.OPTIONAL_STRING_SCHEMA)
            .field("regex", Schema.OPTIONAL_STRING_SCHEMA)
            .field("string", Schema.OPTIONAL_STRING_SCHEMA)
            .field("symbol", Schema.OPTIONAL_STRING_SCHEMA)
            .field("timestamp", Timestamp.builder().optional().build())
            .field("undefined", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    assertSchemaEquals(expected, inferDocumentSchema(bsonDocument));
  }

  @Test
  void testArraysSimple() {
    BsonDocument bsonDocument =
        BsonDocument.parse(
            "{"
                + "empty: [],"
                + "ints: [1, 2, 3],"
                + "intsNull: [1, null, 3],"
                + "intsNullFirst: [null, 1, 3],"
                + "mixedTypes: [1, 'foo', {a: 1}]}");

    Schema expected =
        SchemaBuilder.struct()
            .name(DEFAULT_FIELD_NAME)
            .field("empty", createArray("empty", Schema.OPTIONAL_STRING_SCHEMA))
            .field("ints", createArray("ints", Schema.OPTIONAL_INT32_SCHEMA))
            .field("intsNull", createArray("intsNull", Schema.OPTIONAL_INT32_SCHEMA))
            .field("intsNullFirst", createArray("intsNullFirst", Schema.OPTIONAL_INT32_SCHEMA))
            .field("mixedTypes", createArray("mixedTypes", Schema.OPTIONAL_STRING_SCHEMA))
            .build();

    assertSchemaEquals(expected, inferDocumentSchema(bsonDocument));
  }

  @Test
  void testFieldOrderingHandling() {
    BsonDocument bsonDocument =
        BsonDocument.parse(
            "{"
                + " arrays: [{_a: 'foo', _id: 'foo'}, {_id: 'bar', a: ''}],"
                + "_id: 'foo'"
                + "_a: 'bar'}");

    Schema expected =
        SchemaBuilder.struct()
            .name(DEFAULT_FIELD_NAME)
            .field("_a", Schema.OPTIONAL_STRING_SCHEMA)
            .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field(
                "arrays",
                createArray(
                    "arrays",
                    SchemaBuilder.struct()
                        .name("arrays")
                        .field("_a", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("_id", Schema.OPTIONAL_STRING_SCHEMA)
                        .field("a", Schema.OPTIONAL_STRING_SCHEMA)
                        .build()))
            .build();

    assertSchemaEquals(expected, inferDocumentSchema(bsonDocument));
  }

  @Test
  void testArraysSimpleNesting() {
    BsonDocument bsonDocument =
        BsonDocument.parse(
            "{"
                + " arrays: [[1], [2], [3]],"
                + " arraysEmpty: [[1], [], [2]],"
                + " arraysEmptyFirst: [[], [1], [2]],"
                + " arraysNull: [[1], null, [2]],"
                + " arraysNullFirst: [[1], null, [2]],"
                + " arraysWithMixedTypes: [[1], ['2'], [{a: 1}]]}");

    Schema expected =
        SchemaBuilder.struct()
            .name(DEFAULT_FIELD_NAME)
            .field("arrays", createNestedArray("arrays", Schema.OPTIONAL_INT32_SCHEMA))
            .field("arraysEmpty", createNestedArray("arraysEmpty", Schema.OPTIONAL_INT32_SCHEMA))
            .field(
                "arraysEmptyFirst",
                createNestedArray("arraysEmptyFirst", Schema.OPTIONAL_INT32_SCHEMA))
            .field("arraysNull", createNestedArray("arraysNull", Schema.OPTIONAL_INT32_SCHEMA))
            .field(
                "arraysNullFirst",
                createNestedArray("arraysNullFirst", Schema.OPTIONAL_INT32_SCHEMA))
            .field(
                "arraysWithMixedTypes",
                createNestedArray("arraysWithMixedTypes", Schema.OPTIONAL_STRING_SCHEMA))
            .build();

    assertSchemaEquals(expected, inferDocumentSchema(bsonDocument));
  }

  @Test
  void testArraysWithStructs() {
    BsonDocument bsonDocument =
        BsonDocument.parse(
            "{"
                + " structs: [{a: 1, b: true, c: 'foo'}, {b: false, d: 4, e: {'$numberLong': '5'}}],"
                + " structsEmpty: [{a: 1, b: true}, {}, {c: 'foo'}, {d: 4, e: {'$numberLong': '5'}}],"
                + " structsEmptyFirst: [{}, {a: 1, b: true}, {c: 'foo'}, {d: 4, e: {'$numberLong': '5'}}],"
                + " structsNull: [{a: 1, b: true, c : null, d: null}, null, {d: 4, e: {'$numberLong': '5'}}],"
                + " structsNullFirst: [null, {a: 1, b: true}, {c: 'foo'}, {d: 4, e: {'$numberLong': '5'}}],"
                + " structsOrdering: [{e: {'$numberLong': '5'}, c: 'foo', b: true, d: 4, a: 1}],"
                + " structsWithMixedTypes: [{a: 1, b: 2, c: 3, d: 4, e: 5}, {a: 'a', b: 'b', c: 'c', d: 'd', e: 'e'}]}");

    Schema expected =
        SchemaBuilder.struct()
            .name(DEFAULT_FIELD_NAME)
            .field("structs", createArray("structs", SIMPLE_STRUCT))
            .field("structsEmpty", createArray("structsEmpty", SIMPLE_STRUCT))
            .field("structsEmptyFirst", createArray("structsEmptyFirst", SIMPLE_STRUCT))
            .field("structsNull", createArray("structsNull", SIMPLE_STRUCT))
            .field("structsNullFirst", createArray("structsNullFirst", SIMPLE_STRUCT))
            .field("structsOrdering", createArray("structsOrdering", SIMPLE_STRUCT))
            .field(
                "structsWithMixedTypes",
                createArray("structsWithMixedTypes", SIMPLE_STRUCT_STRINGS))
            .build();

    assertSchemaEquals(expected, inferDocumentSchema(bsonDocument));
  }

  @Test
  void testArraysOfArraysWithStructs() {
    BsonDocument bsonDocument =
        BsonDocument.parse(
            "{"
                + " arrayStructs: [[{a: 1, b: true,} {c: 'foo'}], [{b: false}, {d: 4, e: {'$numberLong': '5'}}]],"
                + " arrayStructsEmpty: [[{a: 1, b: true}], [{}], [{c: 'foo'}], [], [{d: 4, e: {'$numberLong': '5'}}]],"
                + " arrayStructsEmptyFirst: [[{}], [{a: 1, b: true}, {c: 'foo'}], [{d: 4, e: {'$numberLong': '5'}}]],"
                + " arrayStructsNull: [[{a: 1, b: true, c: null, d: null}, null], null, [{d: 4, e: {'$numberLong': '5'}}]],"
                + " arrayStructsNullFirst: [null, [null], [{a: 1, b: true}, {c: 'foo'}, {d: 4, e: {'$numberLong': '5'}}]],"
                + " arrayStructsOrdering: [[{e: {'$numberLong': '5'}, c: 'foo'}], [{b: true}], [{d: 4, a: 1}]],"
                + " arrayStructsWithMixedTypes: [[{a: 1, b: 2, c: 3, d: 4, e: 5}], [{a: 'a', 'b': 'b'}], [{c: 'c', d: 'd', e: 'e'}]]}");

    Schema expected =
        SchemaBuilder.struct()
            .name(DEFAULT_FIELD_NAME)
            .field("arrayStructs", createNestedArray("arrayStructs", SIMPLE_STRUCT))
            .field("arrayStructsEmpty", createNestedArray("arrayStructsEmpty", SIMPLE_STRUCT))
            .field(
                "arrayStructsEmptyFirst",
                createNestedArray("arrayStructsEmptyFirst", SIMPLE_STRUCT))
            .field("arrayStructsNull", createNestedArray("arrayStructsNull", SIMPLE_STRUCT))
            .field(
                "arrayStructsNullFirst", createNestedArray("arrayStructsNullFirst", SIMPLE_STRUCT))
            .field("arrayStructsOrdering", createNestedArray("arrayStructsOrdering", SIMPLE_STRUCT))
            .field(
                "arrayStructsWithMixedTypes",
                createNestedArray("arrayStructsWithMixedTypes", SIMPLE_STRUCT_STRINGS))
            .build();

    assertSchemaEquals(expected, inferDocumentSchema(bsonDocument));
  }

  @Test
  void testArraysWithStructsWithArrays() {
    BsonDocument bsonDocument =
        BsonDocument.parse(
            "{"
                + " structs: [{inner: [{a: 1, b: true}]}, {inner: [{c: 'foo'}]}, {inner: [{b: false, d: 4, e: {'$numberLong': '5'}}]}],"
                + " structsEmpty: [{inner: []}, {inner: [{a: 1, b: true}]}, {inner: []}, {},"
                + "                {inner: [{c: 'foo', d: 4}]}, {inner: [{e: {'$numberLong': '5'}}]}],"
                + " structsEmptyFirst: [{}, {inner: [{a: 1, b: true}]}, {inner: [{c: 'foo', d: 4, e: {'$numberLong': '5'}}]}],"
                + " structsNull: [{inner: [{a: 1, b: true, c: null, d: null}]}, null, {inner: null}, "
                + "               {inner: [{d: 4, e: {'$numberLong': '5'}}]}],"
                + " structsNullFirst: [null, {inner: [{a: 1, b: true}]}, {inner: [{c: 'foo', d: 4, e: {'$numberLong': '5'}}]}],"
                + " structsOrdering: [{inner: [{e: {'$numberLong': '5'}, c:'foo', b: true}]}, {inner: [{d: 4, a: 1}]}],"
                + " structsWithMixedTypes: [{inner: [{a: 1, b: 2, c: 3, d: 4, e: 5}]}, "
                + "                                  {inner: [{a: 'a', b: 'b', c: 'c', d: 'd', e: 'e'}]}]}");

    Schema expected =
        SchemaBuilder.struct()
            .name(DEFAULT_FIELD_NAME)
            .field("structs", createArray("structs", createArray("inner", SIMPLE_STRUCT)))
            .field("structsEmpty", createArray("structsEmpty", createArray("inner", SIMPLE_STRUCT)))
            .field(
                "structsEmptyFirst",
                createArray("structsEmptyFirst", createArray("inner", SIMPLE_STRUCT)))
            .field("structsNull", createArray("structsNull", createArray("inner", SIMPLE_STRUCT)))
            .field(
                "structsNullFirst",
                createArray("structsNullFirst", createArray("inner", SIMPLE_STRUCT)))
            .field(
                "structsOrdering",
                createArray("structsOrdering", createArray("inner", SIMPLE_STRUCT)))
            .field(
                "structsWithMixedTypes",
                createArray("structsWithMixedTypes", createArray("inner", SIMPLE_STRUCT_STRINGS)))
            .build();

    assertSchemaEquals(expected, inferDocumentSchema(bsonDocument));
  }

  @Test
  void testSentinelType() {
    assertEquals(SENTINEL_STRING_TYPE, INCOMPATIBLE_SCHEMA_TYPE);
    assertFalse(isSentinel(INCOMPATIBLE_SCHEMA_TYPE));
    assertTrue(isSentinel(SENTINEL_STRING_TYPE));
  }

  static Schema createArray(final String name, final Schema valueSchema) {
    return SchemaBuilder.array(valueSchema).optional().name(name).build();
  }

  static Schema createNestedArray(final String name, final Schema valueSchema) {
    return SchemaBuilder.array(SchemaBuilder.array(valueSchema).optional().name(name).build())
        .optional()
        .name(name)
        .build();
  }

  private static final Schema SIMPLE_STRUCT =
      SchemaBuilder.struct()
          .name("default")
          .field("a", Schema.OPTIONAL_INT32_SCHEMA)
          .field("b", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .field("c", Schema.OPTIONAL_STRING_SCHEMA)
          .field("d", Schema.OPTIONAL_INT32_SCHEMA)
          .field("e", Schema.OPTIONAL_INT64_SCHEMA)
          .build();

  private static final Schema SIMPLE_STRUCT_STRINGS =
      SchemaBuilder.struct()
          .name("default")
          .field("a", Schema.OPTIONAL_STRING_SCHEMA)
          .field("b", Schema.OPTIONAL_STRING_SCHEMA)
          .field("c", Schema.OPTIONAL_STRING_SCHEMA)
          .field("d", Schema.OPTIONAL_STRING_SCHEMA)
          .field("e", Schema.OPTIONAL_STRING_SCHEMA)
          .build();
}
