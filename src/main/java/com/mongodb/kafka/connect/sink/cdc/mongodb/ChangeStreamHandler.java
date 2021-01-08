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

package com.mongodb.kafka.connect.sink.cdc.mongodb;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.List;
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
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Delete;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Replace;
import com.mongodb.kafka.connect.sink.cdc.mongodb.operations.Update;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public final class ChangeStreamHandler extends CdcHandler {

  private static final String OPERATION_TYPE = "operationType";
  private static final Map<OperationType, ChangeStreamOperation> OPERATIONS =
      unmodifiableMap(
          new HashMap<OperationType, ChangeStreamOperation>() {
            {
              put(OperationType.INSERT, new Replace());
              put(OperationType.REPLACE, new Replace());
              put(OperationType.UPDATE, new Update());
              put(OperationType.DELETE, new Delete());
            }
          });
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamHandler.class);

  public ChangeStreamHandler(final MongoSinkTopicConfig config) {
    super(config);
  }

  public Optional<WriteModel<BsonDocument>> handle(final SinkDocument doc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<WriteModel<BsonDocument>> createWriteModels(final SinkDocument doc) {
    BsonDocument changeStreamDocument = doc.getValueDoc().orElseGet(BsonDocument::new);

    if (!changeStreamDocument.containsKey(OPERATION_TYPE)) {
      return handleError(
          new DataException(
              format(
                  "Error: `%s` field is doc is missing. %s",
                  OPERATION_TYPE, changeStreamDocument.toJson())));
    } else if (!changeStreamDocument.get(OPERATION_TYPE).isString()) {
      return handleError(
          new DataException("Error: Unexpected CDC operation type, should be a string"));
    }

    OperationType operationType =
        OperationType.fromString(changeStreamDocument.get(OPERATION_TYPE).asString().getValue());

    LOGGER.debug("Creating operation handler for: {}", operationType);
    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace("ChangeStream document {}", changeStreamDocument.toJson());
    }

    if (OPERATIONS.containsKey(operationType)) {
      return handleOperation(() -> OPERATIONS.get(operationType).perform(doc));
    }
    LOGGER.warn("Unsupported change stream operation: {}", operationType.getValue());
    return emptyList();
  }

  List<WriteModel<BsonDocument>> handleError(final DataException dataException) {
    if (getConfig().logErrors()) {
      LOGGER.error(dataException.getMessage());
    }
    if (getConfig().tolerateErrors()) {
      return emptyList();
    }
    throw dataException;
  }

  protected List<WriteModel<BsonDocument>> handleOperation(
      final Supplier<List<WriteModel<BsonDocument>>> supplier) {
    try {
      return supplier.get();
    } catch (Exception e) {
      if (getConfig().logErrors()) {
        LOGGER.error("Unable to process operation.", e);
      }
      if (getConfig().tolerateErrors()) {
        return emptyList();
      } else {
        throw e;
      }
    }
  }
}
