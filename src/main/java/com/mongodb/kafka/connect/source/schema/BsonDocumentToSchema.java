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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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

import com.mongodb.kafka.connect.source.MongoSourceTask;

public final class BsonDocumentToSchema {

  static final Logger LOGGER = LoggerFactory.getLogger(MongoSourceTask.class);

  private static final String SENTINEL_FOR_NULL_OR_EMPTY = "";
  private static final String ID_FIELD = "_id";
  private static final Schema DEFAULT_INFER_SCHEMA_TYPE = Schema.OPTIONAL_STRING_SCHEMA;
  public static final String DEFAULT_FIELD_NAME = "default";

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
        BsonArray bsonArray = bsonValue.asArray();
        if (bsonArray.isEmpty()) {
          return SchemaBuilder.array(
                  SchemaBuilder.string()
                      .optional()
                      // Sentinel value added to detect the empty array case.
                      .defaultValue(SENTINEL_FOR_NULL_OR_EMPTY)
                      .build())
              .name(fieldPath)
              .optional()
              .build();
        }
        Schema combinedSchema = inferSchema(fieldPath, bsonArray.get(0));
        for (int i = 1; i < bsonArray.size(); i++) {
          combinedSchema = combine(combinedSchema, inferSchema(fieldPath, bsonArray.get(i)));
          if (combinedSchema == null) {
            break;
          }
        }
        return combinedSchema == null
            ? SchemaBuilder.array(DEFAULT_INFER_SCHEMA_TYPE).name(fieldPath).optional().build()
            : normalizeSchema(
                SchemaBuilder.array(combinedSchema).name(fieldPath).optional().build());
      case BINARY:
        return Schema.OPTIONAL_BYTES_SCHEMA;
      case SYMBOL:
      case STRING:
        return Schema.OPTIONAL_STRING_SCHEMA;
      case NULL:
        return SchemaBuilder.string()
            .optional()
            // Sentinel value added to detect the empty array case.
            .defaultValue(SENTINEL_FOR_NULL_OR_EMPTY)
            .build();
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
    if (firstSchema.equals(secondSchema)) {
      return firstSchema;
    }

    if (firstSchema.type() != secondSchema.type()) {
      LOGGER.debug(
          "Can't combine non-matching schema types: {} and {}",
          firstSchema.type(),
          secondSchema.type());
      return null;
    }

    if (firstSchema.type() != Schema.Type.STRUCT || secondSchema.type() != Schema.Type.STRUCT) {
      LOGGER.debug("Can't combine non-equal schema that are not both structs");
      return null;
    }

    SchemaBuilder builder = SchemaBuilder.struct().name(firstSchema.name()).optional();

    for (Field firstField : firstSchema.fields()) {
      Field secondField = secondSchema.field(firstField.name());
      if (secondField == null || firstField.schema().equals(secondField.schema())) {
        builder.field(firstField.name(), firstField.schema());
      } else if (isSentinelValueSet(firstField.schema())) {
        builder.field(secondField.name(), secondField.schema());
      } else if (isSentinelValueSet(secondField.schema())) {
        builder.field(firstField.name(), firstField.schema());
      } else if (firstField.schema().type() == Schema.Type.STRUCT
          && secondField.schema().type() == Schema.Type.STRUCT) {
        Schema combinedSchema = combine(firstField.schema(), secondField.schema());
        if (combinedSchema == null) {
          LOGGER.debug(
              "Can't combine non-matching struct fields: {} and {}", firstField, secondField);
          return null;
        }
        builder.field(firstField.name(), combinedSchema);
      } else if (firstField.schema().type() == Schema.Type.ARRAY
          && secondField.schema().type() == Schema.Type.ARRAY) {
        if (isSentinelValueSet(secondField.schema().valueSchema())) {
          builder.field(firstField.name(), firstField.schema());
        } else if (isSentinelValueSet(firstField.schema().valueSchema())) {
          builder.field(secondField.name(), secondField.schema());
        } else {
          Schema combinedSchema =
              combine(firstField.schema().valueSchema(), secondField.schema().valueSchema());
          if (combinedSchema == null) {
            LOGGER.debug(
                "Can't combine non-matching array element value schema: {} and {}",
                firstField,
                secondField);
            return null;
          }
          builder.field(
              firstField.name(),
              SchemaBuilder.array(combinedSchema).name(firstField.name()).optional().build());
        }
      } else {
        LOGGER.debug("Can't combine non-matching fields: {} and {}", firstField, secondField);
        return null;
      }
    }

    for (Field secondField : secondSchema.fields()) {
      if (firstSchema.field(secondField.name()) == null) {
        builder.field(secondField.name(), secondField.schema());
      }
    }

    return builder.build();
  }

  // Relies on the sentinel value added above
  private static boolean isSentinelValueSet(final Schema schema) {
    return schema.type() == Schema.Type.STRING
        && SENTINEL_FOR_NULL_OR_EMPTY.equals(schema.defaultValue());
  }

  /**
   * This method normalizes the schema that had been denormalized due to the array element
   * schema-combining logic applied as part of schema building process.
   */
  private static Schema normalizeSchema(final Schema schema) {
    switch (schema.type()) {
      case STRUCT:
        return normalizeStructSchema(schema);
      case ARRAY:
        SchemaBuilder arraySchemaBuilder =
            SchemaBuilder.array(normalizeSchema(schema.valueSchema())).name(schema.name());
        if (schema.isOptional()) {
          arraySchemaBuilder.optional();
        }
        return arraySchemaBuilder.build();
      case STRING:
        SchemaBuilder stringSchemaBuilder = SchemaBuilder.string().name(schema.name());
        if (schema.isOptional()) {
          stringSchemaBuilder.optional();
        }
        return stringSchemaBuilder.build();
      default:
        return schema;
    }
  }

  private static Schema normalizeStructSchema(final Schema schema) {
    SchemaBuilder builder = SchemaBuilder.struct().name(schema.name());
    if (schema.isOptional()) {
      builder.optional();
    }

    List<Field> fields = new ArrayList<>(schema.fields());
    fields.sort(Comparator.comparing(Field::name));

    Field idField = schema.field(ID_FIELD);
    if (idField != null) {
      builder.field(idField.name(), normalizeSchema(idField.schema()));
    }

    for (Field cur : fields) {
      if (cur.name().equals(ID_FIELD)) {
        continue;
      }
      builder.field(cur.name(), normalizeSchema(cur.schema()));
    }

    return builder.build();
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
