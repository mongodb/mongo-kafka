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

import static com.mongodb.kafka.connect.source.schema.SchemaDebugHelper.prettyPrintSchemas;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class BsonDocumentToSchema {
  public static final String DEFAULT_FIELD_NAME = "default";
  private static final Logger LOGGER = LoggerFactory.getLogger(BsonDocumentToSchema.class);
  private static final String ID_FIELD = "_id";
  static final Schema INCOMPATIBLE_SCHEMA_TYPE = Schema.OPTIONAL_STRING_SCHEMA;
  static final Schema SENTINEL_STRING_TYPE =
      SchemaBuilder.type(Schema.Type.STRING).optional().build();

  public static Schema inferDocumentSchema(final BsonDocument document) {
    return createSchemaBuilder(DEFAULT_FIELD_NAME, document).required().build();
  }

  private static Schema inferDocumentSchema(final String fieldPath, final BsonDocument document) {
    return createSchemaBuilder(fieldPath, document).optional().build();
  }

  private static Schema inferArraySchema(final String fieldPath, final BsonArray bsonArray) {
    Schema combinedSchema = SENTINEL_STRING_TYPE;
    for (final BsonValue v : bsonArray) {
      combinedSchema = combinedSchema(combinedSchema, inferSchema(fieldPath, v));
      if (combinedSchema == INCOMPATIBLE_SCHEMA_TYPE) {
        break;
      }
    }
    return SchemaBuilder.array(combinedSchema).name(fieldPath).optional().build();
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
        return inferArraySchema(fieldPath, bsonValue.asArray());
      case BINARY:
        return Schema.OPTIONAL_BYTES_SCHEMA;
      case NULL:
        return SENTINEL_STRING_TYPE;
      case SYMBOL:
      case STRING:
      case OBJECT_ID:
      case REGULAR_EXPRESSION:
      case DB_POINTER:
      case JAVASCRIPT:
      case JAVASCRIPT_WITH_SCOPE:
      case MIN_KEY:
      case MAX_KEY:
      case UNDEFINED:
      default:
        return Schema.OPTIONAL_STRING_SCHEMA;
    }
  }

  private static Schema combinedSchema(final Schema firstSchema, final Schema secondSchema) {
    if (isSentinel(firstSchema)) {
      return secondSchema;
    } else if (isSentinel(secondSchema)) {
      return firstSchema;
    }

    if (firstSchema.equals(secondSchema)) {
      return firstSchema;
    }

    if (firstSchema.type() != secondSchema.type()) {
      logIncompatibleSchemas(firstSchema, secondSchema);
      return INCOMPATIBLE_SCHEMA_TYPE;
    }

    switch (firstSchema.type()) {
      case ARRAY:
        SchemaBuilder arrayBuilder =
            SchemaBuilder.array(
                    combinedSchema(firstSchema.valueSchema(), secondSchema.valueSchema()))
                .name(firstSchema.name())
                .optional();
        return arrayBuilder.build();
      case STRUCT:
        SchemaBuilder structBuilder = SchemaBuilder.struct().name(firstSchema.name()).optional();

        // _id field first
        Field id1 = firstSchema.field(ID_FIELD);
        Field id2 = secondSchema.field(ID_FIELD);
        if (id1 != null || id2 != null) {
          structBuilder.field(ID_FIELD, combineFieldSchema(id1, id2));
        }
        // Combine other fields in name order
        Stream.concat(
                firstSchema.fields().stream().map(Field::name),
                secondSchema.fields().stream().map(Field::name))
            .filter(name -> !name.equals(ID_FIELD))
            .distinct()
            .sorted()
            .forEach(
                name ->
                    structBuilder.field(
                        name,
                        combineFieldSchema(firstSchema.field(name), secondSchema.field(name))));
        return structBuilder.build();
      default:
        // Should be unreachable as the only non-primitive types supported are Arrays & Structs
        logIncompatibleSchemas(firstSchema, secondSchema);
        return INCOMPATIBLE_SCHEMA_TYPE;
    }
  }

  private static Schema combineFieldSchema(final Field firstField, final Field secondField) {
    if (firstField == null) {
      return secondField.schema();
    } else if (secondField == null) {
      return firstField.schema();
    }

    if (firstField.schema().equals(secondField.schema())) {
      return firstField.schema();
    } else if (isSentinel(firstField.schema())) {
      return secondField.schema();
    } else if (isSentinel(secondField.schema())) {
      return firstField.schema();
    }

    return combinedSchema(firstField.schema(), secondField.schema());
  }

  static boolean isSentinel(final Schema schema) {
    return schema == SENTINEL_STRING_TYPE;
  }

  private static String createFieldPath(final String fieldPath, final String fieldName) {
    if (fieldPath.equals(DEFAULT_FIELD_NAME)) {
      return fieldName;
    } else {
      return fieldPath + "_" + fieldName;
    }
  }

  private static void logIncompatibleSchemas(final Schema firstSchema, final Schema secondSchema) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Incompatible Schemas: {}", prettyPrintSchemas(firstSchema, secondSchema));
    }
  }

  private BsonDocumentToSchema() {}
}
