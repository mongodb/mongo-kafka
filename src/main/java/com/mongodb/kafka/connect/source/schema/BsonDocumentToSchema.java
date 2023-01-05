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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class BsonDocumentToSchema {
  public static final String DEFAULT_FIELD_NAME = "default";
  private static final Logger LOGGER = LoggerFactory.getLogger(BsonDocumentToSchema.class);
  private static final String ID_FIELD = "_id";
  private static final Schema DEFAULT_INFER_SCHEMA_TYPE = Schema.OPTIONAL_STRING_SCHEMA;
  private static final Schema SENTINEL_STRING_TYPE =
      SchemaBuilder.type(Schema.Type.STRING).optional().build();

  public static Schema inferDocumentSchema(final BsonDocument document) {
    return createSchemaBuilder(DEFAULT_FIELD_NAME, document).required().build();
  }

  private static Schema inferDocumentSchema(final String fieldPath, final BsonDocument document) {
    return createSchemaBuilder(fieldPath, document).optional().build();
  }

  private static SchemaBuilder createSchemaBuilder(
      final String fieldPath, final BsonDocument document) {
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(fieldPath);
    if (document.containsKey(ID_FIELD)) {
      builder.field(ID_FIELD, inferSchema(ID_FIELD, document.get(ID_FIELD)));
    }
    document.entrySet().stream()
        .filter(kv -> !kv.getKey().equals(ID_FIELD))
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            kv ->
                builder.field(
                    kv.getKey(),
                    inferSchema(createFieldPath(fieldPath, kv.getKey()), kv.getValue())));
    return builder;
  }

  private static Schema inferSchema(final String fieldPath, final BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case BOOLEAN:
        return Schema.OPTIONAL_BOOLEAN_SCHEMA;
      case INT32:
        return Schema.OPTIONAL_INT32_SCHEMA;
      case INT64:
        return Schema.OPTIONAL_INT64_SCHEMA;
      case DOUBLE:
        return Schema.OPTIONAL_FLOAT64_SCHEMA;
      case DECIMAL128:
        return Decimal.builder(bsonValue.asDecimal128().getValue().bigDecimalValue().scale())
            .optional()
            .build();
      case DATE_TIME:
      case TIMESTAMP:
        return Timestamp.builder().optional().build();
      case DOCUMENT:
        return inferDocumentSchema(fieldPath, bsonValue.asDocument());
      case ARRAY:
        List<BsonValue> values = bsonValue.asArray().getValues();
        Schema combinedSchema =
            values.stream()
                .map(i -> inferSchema(fieldPath, i))
                .reduce(SENTINEL_STRING_TYPE, BsonDocumentToSchema::combine);
        return SchemaBuilder.array(combinedSchema).name(fieldPath).optional().build();
      case BINARY:
        return Schema.OPTIONAL_BYTES_SCHEMA;
      case SYMBOL:
      case STRING:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case NULL:
        return SENTINEL_STRING_TYPE;
      case OBJECT_ID:
      case REGULAR_EXPRESSION:
      case DB_POINTER:
      case JAVASCRIPT:
      case JAVASCRIPT_WITH_SCOPE:
      case MIN_KEY:
      case MAX_KEY:
      case UNDEFINED:
      default:
        return DEFAULT_INFER_SCHEMA_TYPE;
    }
  }

  private static Schema combine(final Schema firstSchema, final Schema secondSchema) {
    if (isSentinelValueSet(firstSchema)) {
      return secondSchema;
    }

    if (firstSchema.equals(secondSchema)) {
      return firstSchema;
    }

    if (firstSchema.type() != secondSchema.type()) {
      LOGGER.debug(
          "Can't combine non-matching schema types: {} and {}",
          firstSchema.type(),
          secondSchema.type());
      return DEFAULT_INFER_SCHEMA_TYPE;
    }

    if (firstSchema.type() != Schema.Type.STRUCT || secondSchema.type() != Schema.Type.STRUCT) {
      LOGGER.debug("Can't combine non-equal schema that are not both structs");
      return DEFAULT_INFER_SCHEMA_TYPE;
    }

    SchemaBuilder builder = SchemaBuilder.struct().name(firstSchema.name()).optional();

    // _id field first
    Field _id1 = firstSchema.field(ID_FIELD);
    Field _id2 = secondSchema.field(ID_FIELD);
    if (_id1 != null || _id2 != null) {
      builder.field(ID_FIELD, combineFieldSchema(_id1, _id2));
    }
    // Combine other fields in name order
    Stream.concat(
            firstSchema.fields().stream().map(Field::name),
            secondSchema.fields().stream().map(Field::name))
        .filter(name -> !name.equals(ID_FIELD))
        .sorted()
        .distinct()
        .forEach(
            name ->
                builder.field(
                    name, combineFieldSchema(firstSchema.field(name), secondSchema.field(name))));
    return builder.build();
  }

  private static Schema combineFieldSchema(final Field firstField, final Field secondField) {
    if (firstField == null) {
      return secondField.schema();
    } else if (secondField == null) {
      return firstField.schema();
    }

    if (firstField.schema().equals(secondField.schema())) {
      return firstField.schema();
    } else if (isSentinelValueSet(firstField.schema())) {
      return secondField.schema();
    } else if (isSentinelValueSet(secondField.schema())) {
      return firstField.schema();
    } else if (firstField.schema().type() == Schema.Type.STRUCT
        && secondField.schema().type() == Schema.Type.STRUCT) {
      return combine(firstField.schema(), secondField.schema());
    } else if (firstField.schema().type() == Schema.Type.ARRAY
        && secondField.schema().type() == Schema.Type.ARRAY) {
      if (isSentinelValueSet(secondField.schema().valueSchema())) {
        return firstField.schema();
      } else if (isSentinelValueSet(firstField.schema().valueSchema())) {
        return secondField.schema();
      } else {
        Schema combinedArrayValueSchema =
            combine(firstField.schema().valueSchema(), secondField.schema().valueSchema());
        return SchemaBuilder.array(combinedArrayValueSchema)
            .name(firstField.name())
            .optional()
            .build();
      }
    } else {
      LOGGER.debug("Can't combine non-matching fields: {} and {}", firstField, secondField);
      return DEFAULT_INFER_SCHEMA_TYPE;
    }
  }

  private static boolean isSentinelValueSet(final Schema schema) {
    return schema == SENTINEL_STRING_TYPE;
  }

  private static String createFieldPath(final String fieldPath, final String fieldName) {
    if (fieldPath.equals(DEFAULT_FIELD_NAME)) {
      return fieldName;
    } else {
      return fieldPath + "_" + fieldName;
    }
  }

  private BsonDocumentToSchema() {}
}
