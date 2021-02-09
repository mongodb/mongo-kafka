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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink.cdc.debezium.mongodb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.debezium.DebeziumCdcHandler;
import com.mongodb.kafka.connect.sink.cdc.debezium.OperationType;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class MongoDbHandler extends DebeziumCdcHandler {
  static final String ID_FIELD = "_id";
  static final String JSON_ID_FIELD = "id";
  private static final Map<OperationType, CdcOperation> DEFAULT_OPERATIONS =
      new HashMap<OperationType, CdcOperation>() {
        {
          put(OperationType.CREATE, new MongoDbInsert());
          put(OperationType.READ, new MongoDbInsert());
          put(OperationType.UPDATE, new MongoDbUpdate());
          put(OperationType.DELETE, new MongoDbDelete());
        }
      };
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbHandler.class);

  public MongoDbHandler(final MongoSinkTopicConfig config) {
    this(config, DEFAULT_OPERATIONS);
  }

  public MongoDbHandler(
      final MongoSinkTopicConfig config, final Map<OperationType, CdcOperation> operations) {
    super(config);
    registerOperations(operations);
  }

  @Override
  public Optional<WriteModel<BsonDocument>> handle(final SinkDocument doc) {

    BsonDocument keyDoc = doc.getKeyDoc().orElseGet(BsonDocument::new);

    if (keyDoc.isEmpty()) {
      if (getConfig().logErrors()) {
        LOGGER.error("Key document must not be missing for CDC mode {}", doc);
      }
      if (getConfig().tolerateErrors()) {
        return Optional.empty();
      }
      throw new DataException("Key document must not be missing for CDC mode");
    }

    BsonDocument valueDoc = doc.getValueDoc().orElseGet(BsonDocument::new);

    if (keyDoc.containsKey(JSON_ID_FIELD) && valueDoc.isEmpty()) {
      LOGGER.debug("Skipping debezium tombstone event for kafka topic compaction");
      return Optional.empty();
    }

    LOGGER.debug("key: " + keyDoc.toString());
    LOGGER.debug("value: " + valueDoc.toString());

    return handleOperation(() -> Optional.of(getCdcOperation(valueDoc).perform(doc)));
  }
}
