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
import java.util.Objects;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

import org.bson.BsonValue;

public final class BsonDocumentToSchema {

  private static final Schema DEFAULT_INFER_SCHEMA_TYPE = Schema.STRING_SCHEMA;
  public static final String SCHEMA_NAME_TEMPLATE = "inferred_name_%s";

  public static Schema inferSchema(final BsonValue bsonValue) {
    switch (bsonValue.getBsonType()) {
      case BOOLEAN:
        return Schema.BOOLEAN_SCHEMA;
      case INT32:
        return Schema.INT32_SCHEMA;
      case INT64:
        return Schema.INT64_SCHEMA;
      case DOUBLE:
        return Schema.FLOAT64_SCHEMA;
      case DECIMAL128:
        return Decimal.schema(bsonValue.asDecimal128().getValue().bigDecimalValue().scale());
      case DATE_TIME:
      case TIMESTAMP:
        return Timestamp.SCHEMA;
      case DOCUMENT:
        SchemaBuilder builder = SchemaBuilder.struct();
        bsonValue.asDocument().forEach((k, v) -> builder.field(k, inferSchema(v)));
        builder.name(generateName(builder));
        return builder.build();
      case ARRAY:
        List<BsonValue> values = bsonValue.asArray().getValues();
        Schema firstItemSchema =
            values.isEmpty() ? DEFAULT_INFER_SCHEMA_TYPE : inferSchema(values.get(0));
        if (values.stream().anyMatch(bv -> inferSchema(bv) != firstItemSchema)) {
          return SchemaBuilder.array(DEFAULT_INFER_SCHEMA_TYPE).build();
        }
        return SchemaBuilder.array(inferSchema(bsonValue.asArray().getValues().get(0))).build();
      case BINARY:
        return Schema.BYTES_SCHEMA;
      case SYMBOL:
      case STRING:
        return Schema.STRING_SCHEMA;
      case OBJECT_ID:
      case NULL:
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

  private static String generateName(final SchemaBuilder builder) {
    return format(SCHEMA_NAME_TEMPLATE, Objects.hashCode(builder.build())).replace("-", "_");
  }

  private BsonDocumentToSchema() {}
}
