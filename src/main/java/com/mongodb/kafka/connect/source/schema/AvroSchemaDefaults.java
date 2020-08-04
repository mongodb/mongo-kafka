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

import org.apache.kafka.connect.data.Schema;

public final class AvroSchemaDefaults {

  public static final String DEFAULT_AVRO_KEY_SCHEMA =
      "{"
          + "  \"type\": \"record\","
          + "  \"name\": \"keySchema\","
          + "  \"fields\" : [{\"name\": \"_id\", \"type\": \"string\"}]"
          + "}";

  public static final String DEFAULT_AVRO_VALUE_SCHEMA =
      "{"
          + "  \"name\": \"ChangeStream\","
          + "  \"type\": \"record\","
          + "  \"fields\": ["
          + "    { \"name\": \"_id\", \"type\": \"string\" },"
          + "    { \"name\": \"operationType\", \"type\": [\"string\", \"null\"] },"
          + "    { \"name\": \"fullDocument\", \"type\": [\"string\", \"null\"] },"
          + "    { \"name\": \"ns\","
          + "      \"type\": [{\"name\": \"ns\", \"type\": \"record\", \"fields\": ["
          + "                {\"name\": \"db\", \"type\": \"string\"},"
          + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
          + "               }, \"null\" ] },"
          + "    { \"name\": \"to\","
          + "      \"type\": [{\"name\": \"to\", \"type\": \"record\",  \"fields\": ["
          + "                {\"name\": \"db\", \"type\": \"string\"},"
          + "                {\"name\": \"coll\", \"type\": [\"string\", \"null\"] } ]"
          + "               }, \"null\" ] },"
          + "    { \"name\": \"documentKey\", \"type\": [\"string\", \"null\"] },"
          + "    { \"name\": \"updateDescription\","
          + "      \"type\": [{\"name\": \"updateDescription\",  \"type\": \"record\", \"fields\": ["
          + "                 {\"name\": \"updatedFields\", \"type\": [\"string\", \"null\"]},"
          + "                 {\"name\": \"removedFields\","
          + "                  \"type\": [{\"type\": \"array\", \"items\": \"string\"}, \"null\"]"
          + "                  }] }, \"null\"] },"
          + "    { \"name\": \"clusterTime\", \"type\": [\"string\", \"null\"] },"
          + "    { \"name\": \"txnNumber\", \"type\": [\"long\", \"null\"]},"
          + "    { \"name\": \"lsid\", \"type\": [{\"name\": \"lsid\", \"type\": \"record\","
          + "               \"fields\": [ {\"name\": \"id\", \"type\": \"string\"},"
          + "                             {\"name\": \"uid\", \"type\": \"string\"}] }, \"null\"] }"
          + "  ]"
          + "}";

  public static final Schema DEFAULT_KEY_SCHEMA = AvroSchema.fromJson(DEFAULT_AVRO_KEY_SCHEMA);

  public static final Schema DEFAULT_VALUE_SCHEMA = AvroSchema.fromJson(DEFAULT_AVRO_VALUE_SCHEMA);

  private AvroSchemaDefaults() {}
}
