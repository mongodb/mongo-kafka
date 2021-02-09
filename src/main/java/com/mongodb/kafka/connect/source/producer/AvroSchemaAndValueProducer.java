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

package com.mongodb.kafka.connect.source.producer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;

import org.bson.BsonDocument;
import org.bson.json.JsonWriterSettings;

import com.mongodb.kafka.connect.source.schema.AvroSchema;
import com.mongodb.kafka.connect.source.schema.BsonValueToSchemaAndValue;

final class AvroSchemaAndValueProducer implements SchemaAndValueProducer {
  private final Schema schema;
  private final BsonValueToSchemaAndValue bsonValueToSchemaAndValue;

  AvroSchemaAndValueProducer(final String jsonSchema, final JsonWriterSettings jsonWriterSettings) {
    schema = AvroSchema.fromJson(jsonSchema);
    bsonValueToSchemaAndValue = new BsonValueToSchemaAndValue(jsonWriterSettings);
  }

  @Override
  public SchemaAndValue get(final BsonDocument changeStreamDocument) {
    return bsonValueToSchemaAndValue.toSchemaAndValue(schema, changeStreamDocument);
  }
}
