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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

/**
 * topic.mapper = "com.mongodb.kafka.connect.source.topic.mapping.BinaryTopicMapper"
 * output.format.value = "BINARY_OUTBOX" output.format.key = "BINARY_OUTBOX"
 * binary_outbox.document.topic = "topic" binary_outbox.document.key = "key"
 * binary_outbox.document.value = "value"
 */
public class BinaryOutboxSchemaAndValueProducer implements SchemaAndValueProducer {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BinaryOutboxSchemaAndValueProducer.class);

  public static final String KEY_CONFIG = "binary_outbox.document.key";
  public static final String KEY_CONFIG_DEFAULT = "key";
  public static final String KEY_CONFIG_DOC = "binary_outbox.document.key";
  public static final String KEY_CONFIG_DISPLAY = "binary_outbox.document.key";

  public static final String VALUE_CONFIG = "binary_outbox.document.value";
  public static final String VALUE_CONFIG_DEFAULT = "message";
  public static final String VALUE_CONFIG_DOC = "binary_outbox.document.value";
  public static final String VALUE_CONFIG_DISPLAY = "binary_outbox.document.value";

  private final boolean isValue;

  private final String fieldName;

  BinaryOutboxSchemaAndValueProducer(final String fieldName, final boolean isValue) {
    this.fieldName = fieldName;
    this.isValue = isValue;
  }

  @Override
  public SchemaAndValue get(final BsonDocument changeStreamDocument) {
    if (!changeStreamDocument.containsKey("fullDocument")) {
      LOGGER.error(
          "document does not contain a topic field named 'fullDocument': {}",
          changeStreamDocument.toJson());
      return new SchemaAndValue(Schema.BYTES_SCHEMA, null);
    }

    BsonDocument fullDocument = changeStreamDocument.getDocument("fullDocument");

    if (!fullDocument.containsKey(fieldName)) {
      LOGGER.error("document does not contain a field named '{}'", fieldName);
      return new SchemaAndValue(Schema.BYTES_SCHEMA, null);
    }

    if (isValue) {
      LOGGER.info("value '{}' = {}", fieldName, fullDocument.getBinary(fieldName).getData());
      return new SchemaAndValue(Schema.BYTES_SCHEMA, fullDocument.getBinary(fieldName).getData());
    } else {
      LOGGER.info("key '{}' = {}", fieldName, fullDocument.getString(fieldName).getValue());
      return new SchemaAndValue(Schema.STRING_SCHEMA, fullDocument.getString(fieldName).getValue());
    }
  }
}
