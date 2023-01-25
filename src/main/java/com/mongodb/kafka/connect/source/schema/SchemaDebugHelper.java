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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public final class SchemaDebugHelper {

  public static String prettyPrintSchema(final String name, final Schema schema) {
    return appendSchemaInformation(new StringBuilder(), name, schema).toString();
  }

  static String prettyPrintSchemas(final Schema firstSchema, final Schema secondSchema) {
    StringBuilder builder = new StringBuilder();
    builder.append(firstSchema.type());
    builder.append(" : ");
    builder.append(secondSchema.type());

    if (firstSchema.type().isPrimitive() && secondSchema.type().isPrimitive()) {
      return builder.toString();
    }

    builder.append("\n");
    appendSchemaInformation(builder, "Schema one", firstSchema);
    appendSchemaInformation(builder, "Schema two", secondSchema);
    return builder.toString();
  }

  public static StringBuilder appendSchemaInformation(
      final StringBuilder builder, final String name, final Schema schema) {
    builder.append(name);
    builder.append(":\n");
    appendSchemaInformation(builder, schema, 0);
    return builder;
  }

  private static void appendSchemaInformation(
      final StringBuilder builder, final Schema schema, final int level) {
    String padding = createPadding(level);
    if (level > 10) {
      builder.append(padding);
      builder.append(" ... Very high level of nesting ...");
      return;
    }

    switch (schema.type()) {
      case ARRAY:
        builder.append(padding);

        StringBuilder arrayPostfix = new StringBuilder();
        builder.append(" [");
        arrayPostfix.append(" ]");
        Schema valueSchema = schema.valueSchema();

        while (valueSchema.type() == Schema.Type.ARRAY) {
          builder.append(" [");
          arrayPostfix.append(" ]");
          valueSchema = valueSchema.valueSchema();
        }

        builder.append("\n");
        appendSchemaInformation(builder, valueSchema, level);

        builder.append(padding);
        builder.append(arrayPostfix);
        builder.append("\n");
        break;
      case STRUCT:
        schema.fields().forEach(f -> appendFieldInformation(builder, f, level + 1));
        break;
      default:
        builder.append(padding);
        builder.append(schema.type().getName());
        builder.append("\n");
    }
  }

  private static void appendFieldInformation(
      final StringBuilder builder, final Field field, final int level) {
    String padding = createPadding(level);
    builder.append(padding);
    builder.append(field.name());
    builder.append(": ");
    builder.append(field.schema().type().getName());
    builder.append(" (optional = ");
    builder.append(field.schema().isOptional());
    builder.append(")");
    builder.append(" (name = ");
    builder.append(field.schema().name());
    builder.append(")");
    builder.append("\n");

    switch (field.schema().type()) {
      case ARRAY:
      case STRUCT:
        appendSchemaInformation(builder, field.schema(), level);
        break;
      default:
        break;
    }
  }

  private static String createPadding(final int level) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(" ");
    for (int i = 1; i <= level; i++) {
      stringBuilder.append(" | ");
    }
    return stringBuilder.toString();
  }

  private SchemaDebugHelper() {}
}
