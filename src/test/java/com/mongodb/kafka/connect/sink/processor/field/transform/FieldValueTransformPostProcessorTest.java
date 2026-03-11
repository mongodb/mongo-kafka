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

package com.mongodb.kafka.connect.sink.processor.field.transform;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FAIL_ON_ERROR_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

/** Unit tests for {@link FieldValueTransformPostProcessor}. */
class FieldValueTransformPostProcessorTest {

  /** A simple test transformer that uppercases string values. */
  public static class UpperCaseTransformer implements FieldValueTransformer {
    UpperCaseTransformer() {}

    @Override
    public void init(final Map<String, String> configs) {}

    @Override
    public BsonValue transform(final String fieldName, final BsonValue value) {
      if (value.isString()) {
        return new BsonString(value.asString().getValue().toUpperCase());
      }
      return value;
    }
  }

  /** A transformer that always throws to test error handling. */
  public static class FailingTransformer implements FieldValueTransformer {
    FailingTransformer() {}

    @Override
    public void init(final Map<String, String> configs) {}

    @Override
    public BsonValue transform(final String fieldName, final BsonValue value) {
      throw new RuntimeException("Intentional failure for field: " + fieldName);
    }
  }

  @Test
  @DisplayName("test transforms configured fields")
  void testTransformsConfiguredFields() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name,city'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = BsonDocument.parse("{'name': 'alice', 'age': 30, 'city': 'london'}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals("ALICE", result.getString("name").getValue());
    assertEquals("LONDON", result.getString("city").getValue());
    assertEquals(30, result.getInt32("age").getValue());
  }

  @Test
  @DisplayName("test transforms nested document fields")
  void testTransformsNestedFields() {
    String json =
        format(
            "{'%s': '%s', '%s': 'secret'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = BsonDocument.parse("{'data': {'secret': 'hidden', 'visible': 'ok'}}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals("HIDDEN", result.getDocument("data").getString("secret").getValue());
    assertEquals("ok", result.getDocument("data").getString("visible").getValue());
  }

  @Test
  @DisplayName("test fail on error throws DataException")
  void testFailOnError() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name', '%s': true}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            FailingTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG,
            FIELD_VALUE_TRANSFORMER_FAIL_ON_ERROR_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = BsonDocument.parse("{'name': 'test'}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    assertThrows(DataException.class, () -> processor.process(sinkDoc, null));
  }

  @Test
  @DisplayName("test no fail on error keeps original value")
  void testNoFailOnErrorKeepsOriginal() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name', '%s': false}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            FailingTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG,
            FIELD_VALUE_TRANSFORMER_FAIL_ON_ERROR_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = BsonDocument.parse("{'name': 'original'}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals("original", result.getString("name").getValue());
  }

  @Test
  @DisplayName("test skips fields not in target list")
  void testSkipsNonTargetFields() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = BsonDocument.parse("{'name': 'alice', 'untouched': 'stays lowercase'}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals("ALICE", result.getString("name").getValue());
    assertEquals("stays lowercase", result.getString("untouched").getValue());
  }

  @Test
  @DisplayName("test handles missing target fields gracefully")
  void testHandlesMissingTargetFields() {
    String json =
        format(
            "{'%s': '%s', '%s': 'nonexistent'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = BsonDocument.parse("{'name': 'alice'}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals("alice", result.getString("name").getValue());
  }

  @Test
  @DisplayName("test handles empty document")
  void testHandlesEmptyDocument() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = new BsonDocument();
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertTrue(result.isEmpty());
  }

  @Test
  @DisplayName("test handles null value document")
  void testHandlesNullValueDoc() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    SinkDocument sinkDoc = new SinkDocument(null, null);
    // should not throw
    processor.process(sinkDoc, null);
  }

  @Test
  @DisplayName("test transforms fields inside arrays of documents")
  void testTransformsFieldsInArrays() {
    String json =
        format(
            "{'%s': '%s', '%s': 'secret'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc =
        BsonDocument.parse(
            "{'items': [{'secret': 'a', 'other': 'x'}, {'secret': 'b', 'other': 'y'}]}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals("A", result.getArray("items").get(0).asDocument().getString("secret").getValue());
    assertEquals("B", result.getArray("items").get(1).asDocument().getString("secret").getValue());
    assertEquals("x", result.getArray("items").get(0).asDocument().getString("other").getValue());
    assertEquals("y", result.getArray("items").get(1).asDocument().getString("other").getValue());
  }

  @Test
  @DisplayName("test transforms deeply nested fields")
  void testTransformsDeeplyNestedFields() {
    String json =
        format(
            "{'%s': '%s', '%s': 'target'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc =
        BsonDocument.parse("{'level1': {'level2': {'level3': {'target': 'deep'}}}}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals(
        "DEEP",
        result
            .getDocument("level1")
            .getDocument("level2")
            .getDocument("level3")
            .getString("target")
            .getValue());
  }

  @Test
  @DisplayName("test transforms multiple fields across nesting levels")
  void testTransformsMultipleFieldsAcrossLevels() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name,city'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            UpperCaseTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc =
        BsonDocument.parse(
            "{'name': 'alice', 'address': {'city': 'london', 'zip': '12345'}, 'age': 30}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    processor.process(sinkDoc, null);

    BsonDocument result = sinkDoc.getValueDoc().orElseThrow(IllegalStateException::new);
    assertEquals("ALICE", result.getString("name").getValue());
    assertEquals("LONDON", result.getDocument("address").getString("city").getValue());
    assertEquals("12345", result.getDocument("address").getString("zip").getValue());
    assertEquals(30, result.getInt32("age").getValue());
  }

  @Test
  @DisplayName("test invalid transformer class throws DataException")
  void testInvalidTransformerClassThrows() {
    String json =
        format(
            "{'%s': 'com.example.NonExistentTransformer', '%s': 'name'}",
            FIELD_VALUE_TRANSFORMER_CONFIG, FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    assertThrows(DataException.class, () -> createTopicConfig(json));
  }

  @Test
  @DisplayName("test fail on error defaults to true")
  void testFailOnErrorDefaultsTrue() {
    String json =
        format(
            "{'%s': '%s', '%s': 'name'}",
            FIELD_VALUE_TRANSFORMER_CONFIG,
            FailingTransformer.class.getName(),
            FIELD_VALUE_TRANSFORMER_FIELDS_CONFIG);

    FieldValueTransformPostProcessor processor =
        new FieldValueTransformPostProcessor(createTopicConfig(json));

    BsonDocument valueDoc = BsonDocument.parse("{'name': 'test'}");
    SinkDocument sinkDoc = new SinkDocument(null, valueDoc);

    // Default is fail.on.error=true, so it should throw
    assertThrows(DataException.class, () -> processor.process(sinkDoc, null));
  }
}
