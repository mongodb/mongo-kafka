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

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

import org.bson.BsonDocument;
import org.bson.BsonValue;

public final class BsonDocumentToSchema {

  private static final String ID_FIELD = "_id";
  private static final Schema DEFAULT_INFER_SCHEMA_TYPE = Schema.OPTIONAL_STRING_SCHEMA;
  public static final String SCHEMA_NAME_TEMPLATE = "inferred_name_%s";

  public static Schema inferDocumentSchema(final BsonDocument document, final boolean optional) {
    SchemaBuilder builder = SchemaBuilder.struct();
    if (document.containsKey(ID_FIELD)) {
      builder.field(ID_FIELD, inferSchema(document.get(ID_FIELD)));
    }
    document.entrySet().stream()
        .filter(kv -> !kv.getKey().equals(ID_FIELD))
        .sorted(Map.Entry.comparingByKey())
        .forEach(kv -> builder.field(kv.getKey(), inferSchema(kv.getValue())));
    builder.name(generateName(builder));
    if (optional) {
      builder.optional();
    }
    return builder.build();
  }

  public static Schema inferSchema(final BsonValue bsonValue) {
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
        return inferDocumentSchema(bsonValue.asDocument(), true);
      case ARRAY:
        List<BsonValue> values = bsonValue.asArray().getValues();
        Schema firstItemSchema =
            values.isEmpty() ? DEFAULT_INFER_SCHEMA_TYPE : inferSchema(values.get(0));
        if (values.isEmpty()
            || values.stream().anyMatch(bv -> !Objects.equals(inferSchema(bv), firstItemSchema))) {
          return SchemaBuilder.array(DEFAULT_INFER_SCHEMA_TYPE).optional().build();
        }
        return SchemaBuilder.array(inferSchema(bsonValue.asArray().getValues().get(0)))
            .optional()
            .build();
      case BINARY:
        return Schema.OPTIONAL_BYTES_SCHEMA;
      case SYMBOL:
      case STRING:
      case NULL:
        return Schema.OPTIONAL_STRING_SCHEMA;
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

  public static String generateName(final SchemaBuilder builder) {
    return format(SCHEMA_NAME_TEMPLATE, Objects.hashCode(builder.build())).replace("-", "_");
  }

  private BsonDocumentToSchema() {}
}
