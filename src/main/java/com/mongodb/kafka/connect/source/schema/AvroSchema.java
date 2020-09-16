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

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.avro.Schema.Parser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

public final class AvroSchema {
  public static org.apache.avro.Schema validateJsonSchema(final String jsonSchema) {
    org.apache.avro.Schema avroSchema = parseSchema(jsonSchema);
    if (avroSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
      throw new ConnectException("Only Record schemas are supported at the top-level.");
    }
    validateAvroSchema(avroSchema, "", new ArrayList<>());
    return avroSchema;
  }

  private static void validateAvroSchema(
      final org.apache.avro.Schema avroSchema,
      final String fieldPath,
      final List<String> recordList) {
    switch (avroSchema.getType()) {
      case RECORD:
        if (!recordList.contains(avroSchema.getFullName())) {
          recordList.add(avroSchema.getFullName());
          avroSchema
              .getFields()
              .forEach(
                  f -> {
                    String newFieldPath =
                        fieldPath.isEmpty() ? f.name() : format("%s.%s", fieldPath, f.name());
                    validateAvroSchema(f.schema(), newFieldPath, recordList);
                  });
        }
        break;
      case ARRAY:
        validateAvroSchema(avroSchema.getElementType(), fieldPath, recordList);
        break;
      case MAP:
        validateAvroSchema(avroSchema.getValueType(), fieldPath, recordList);
        break;
      case UNION:
        if (avroSchema.getTypes().size() != 2
            || avroSchema.getTypes().stream()
                .noneMatch(s -> s.getType() == org.apache.avro.Schema.Type.NULL)) {
          throw createConnectException(
              "Union Schemas are not supported, unless one value is "
                  + "null to represent an optional value.",
              fieldPath);
        }
        break;
      case FIXED:
        throw createConnectException(
            format(
                "Unsupported Avro schema type: '%s'. The connector will not validate the length. "
                    + "Use bytes instead.",
                avroSchema.getType()),
            fieldPath);
      case ENUM:
        throw createConnectException(
            format(
                "Unsupported Avro schema type: '%s'. The connector will not validate the values. "
                    + "Use string instead.",
                avroSchema.getType()),
            fieldPath);
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        return;
      case NULL:
      default:
        throw createConnectException(
            format("Unsupported Avro schema type: '%s'.", avroSchema.getType()), fieldPath);
    }
  }

  public static Schema fromJson(final String jsonSchema) {
    org.apache.avro.Schema parsedSchema = validateJsonSchema(jsonSchema);
    return createSchema(parsedSchema);
  }

  static org.apache.avro.Schema parseSchema(final String jsonSchema) {
    try {
      return new Parser().setValidate(false).parse(jsonSchema);
    } catch (Exception e) {
      throw new ConnectException(format("Invalid Avro schema. %s\n%s", e.getMessage(), jsonSchema));
    }
  }

  static Schema createSchema(final org.apache.avro.Schema avroSchema) {
    return createSchema(avroSchema, false, new Context());
  }

  static Schema createSchema(
      final org.apache.avro.Schema avroSchema, final boolean isOptional, final Context context) {
    return createSchema(avroSchema, isOptional, null, context);
  }

  static Schema createSchema(
      final org.apache.avro.Schema avroSchema,
      final boolean isOptional,
      final Object defaultValue,
      final Context context) {
    SchemaBuilder builder;
    switch (avroSchema.getType()) {
      case RECORD:
        SchemaBuilder structBuilder = SchemaBuilder.struct();
        context.schemaCache.put(avroSchema, structBuilder);
        structBuilder.name(avroSchema.getName());
        avroSchema
            .getFields()
            .forEach(
                f -> {
                  if (context.schemaCache.containsKey(f.schema())) {
                    context.detectedCycles.add(f.schema());
                    structBuilder.field(f.name(), context.schemaCache.get(f.schema()));
                  } else {
                    Schema fieldSchema = createSchema(f.schema(), false, f.defaultVal(), context);
                    structBuilder.field(f.name(), fieldSchema);
                  }
                });
        builder = structBuilder;
        break;
      case MAP:
        builder =
            SchemaBuilder.map(
                Schema.STRING_SCHEMA,
                createSchemaCheckCycles(avroSchema.getValueType(), defaultValue, context));
        break;
      case ARRAY:
        builder =
            SchemaBuilder.array(
                createSchemaCheckCycles(avroSchema.getElementType(), defaultValue, context));
        break;
      case STRING:
        builder = SchemaBuilder.string();
        break;
      case BYTES:
      case FIXED:
        builder = SchemaBuilder.bytes();
        break;
      case INT:
        builder = SchemaBuilder.int32();
        break;
      case LONG:
        builder = SchemaBuilder.int64();
        break;
      case FLOAT:
        builder = SchemaBuilder.float32();
        break;
      case DOUBLE:
        builder = SchemaBuilder.float64();
        break;
      case BOOLEAN:
        builder = SchemaBuilder.bool();
        break;
      case UNION:
        Optional<org.apache.avro.Schema> optionalSchema =
            avroSchema.getTypes().stream()
                .filter(s -> s.getType() != org.apache.avro.Schema.Type.NULL)
                .findFirst();
        if (optionalSchema.isPresent()) {
          return createSchema(optionalSchema.get(), true, context);
        }
        throw new IllegalStateException();
      case NULL:
      case ENUM:
      default:
        throw new IllegalStateException();
    }

    if (isOptional) {
      builder.optional();
    }

    if (defaultValue != null) {
      builder.defaultValue(processDefaultValue(builder, defaultValue));
    }

    if (!context.detectedCycles.contains(avroSchema)) {
      context.schemaCache.remove(avroSchema);
    }

    return builder.build();
  }

  static Object processDefaultValue(final SchemaBuilder schemaBuilder, final Object value) {
    if (schemaBuilder.type() == Type.STRUCT) {
      Struct structValue = new Struct(schemaBuilder);
      if (value instanceof Map) {
        Map<?, ?> defaultMap = (Map<?, ?>) value;
        structValue
            .schema()
            .fields()
            .forEach(
                f -> {
                  if (defaultMap.containsKey(f.name())) {
                    structValue.put(f, defaultMap.get(f.name()));
                  }
                });
      }
      return structValue;
    }
    return value;
  }

  static Schema createSchemaCheckCycles(
      final org.apache.avro.Schema avroSchema, final Object defaultValue, final Context context) {
    Schema resolvedSchema;
    if (context.schemaCache.containsKey(avroSchema)) {
      context.detectedCycles.add(avroSchema);
      resolvedSchema = context.schemaCache.get(avroSchema).schema();
    } else {
      resolvedSchema = createSchema(avroSchema, false, defaultValue, context);
    }
    return resolvedSchema;
  }

  private static ConnectException createConnectException(
      final String message, final String fieldPath) {
    String errorMessage = message;
    if (!fieldPath.isEmpty()) {
      errorMessage = format("Field '%s' is invalid. %s", fieldPath, message);
    }
    return new ConnectException(errorMessage);
  }

  private static final class Context {
    private final Map<org.apache.avro.Schema, Schema> schemaCache;
    private final Set<org.apache.avro.Schema> detectedCycles;

    /**
     * schemaCache - map that caches connect Schema references to resolve any schema cycles
     * detectedCycles - avro schemas that have been detected to have cycles
     */
    private Context() {
      this.schemaCache = new IdentityHashMap<>();
      this.detectedCycles = new HashSet<>();
    }
  }

  private AvroSchema() {}
}
