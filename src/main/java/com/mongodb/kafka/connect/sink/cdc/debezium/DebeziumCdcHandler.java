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

package com.mongodb.kafka.connect.sink.cdc.debezium;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;

public abstract class DebeziumCdcHandler extends CdcHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumCdcHandler.class);

  private static final String OPERATION_TYPE_FIELD_PATH = "op";

  private final Map<OperationType, CdcOperation> operations = new HashMap<>();

  public DebeziumCdcHandler(final MongoSinkTopicConfig config) {
    super(config);
  }

  protected void registerOperations(final Map<OperationType, CdcOperation> operations) {
    this.operations.putAll(operations);
  }

  public CdcOperation getCdcOperation(final BsonDocument doc) {
    try {
      if (!doc.containsKey(OPERATION_TYPE_FIELD_PATH)
          || !doc.get(OPERATION_TYPE_FIELD_PATH).isString()) {
        throw new DataException("Value document is missing or CDC operation is not a string");
      }
      CdcOperation op =
          operations.get(
              OperationType.fromText(doc.get(OPERATION_TYPE_FIELD_PATH).asString().getValue()));
      if (op == null) {
        throw new DataException(
            "No CDC operation found in mapping for op="
                + doc.get(OPERATION_TYPE_FIELD_PATH).asString().getValue());
      }
      return op;
    } catch (IllegalArgumentException exc) {
      throw new DataException("Parsing CDC operation failed", exc);
    }
  }

  protected Optional<WriteModel<BsonDocument>> handleOperation(
      final Supplier<Optional<WriteModel<BsonDocument>>> supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      if (getConfig().logErrors()) {
        LOGGER.error("Unable to process operation.", e);
      }
      if (getConfig().tolerateErrors()) {
        return Optional.empty();
      } else {
        throw e;
      }
    }
  }
}
