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

import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_INFER_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_KEY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_VALUE_CONFIG;

import org.apache.kafka.connect.errors.ConnectException;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;

public final class SchemaAndValueProducers {

  public static SchemaAndValueProducer createKeySchemaAndValueProvider(
      final MongoSourceConfig config) {
    return createSchemaAndValueProvider(config, false);
  }

  public static SchemaAndValueProducer createValueSchemaAndValueProvider(
      final MongoSourceConfig config) {
    return createSchemaAndValueProvider(config, true);
  }

  private static SchemaAndValueProducer createSchemaAndValueProvider(
      final MongoSourceConfig config, final boolean isValue) {
    OutputFormat outputFormat =
        isValue ? config.getValueOutputFormat() : config.getKeyOutputFormat();
    switch (outputFormat) {
      case JSON:
        return new RawJsonStringSchemaAndValueProducer(config.getJsonWriterSettings());
      case BSON:
        return new BsonSchemaAndValueProducer();
      case SCHEMA:
        String jsonSchema =
            isValue
                ? config.getString(OUTPUT_SCHEMA_VALUE_CONFIG)
                : config.getString(OUTPUT_SCHEMA_KEY_CONFIG);
        if (isValue && config.getBoolean(OUTPUT_SCHEMA_INFER_VALUE_CONFIG)) {
          return new InferSchemaAndValueProducer(config.getJsonWriterSettings());
        }
        return new AvroSchemaAndValueProducer(jsonSchema, config.getJsonWriterSettings());
      default:
        throw new ConnectException("Unsupported key output format" + config.getKeyOutputFormat());
    }
  }

  private SchemaAndValueProducers() {}
}
