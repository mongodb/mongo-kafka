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

package com.mongodb.kafka.connect.sink.cdc.qlik.rdbms;

import static com.mongodb.kafka.connect.sink.cdc.qlik.rdbms.operations.OperationHelper.DEFAULT_OPERATIONS;
import static java.lang.String.format;

import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.cdc.CdcOperation;
import com.mongodb.kafka.connect.sink.cdc.qlik.OperationType;
import com.mongodb.kafka.connect.sink.cdc.qlik.QlikCdcHandler;
import com.mongodb.kafka.connect.sink.cdc.qlik.rdbms.operations.OperationHelper;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

/**
 * The QLIK Replicate RDBMS Handler
 *
 * <p>Can be extended for specific database customizations
 *
 * @since 1.5.0
 */
public class RdbmsHandler extends QlikCdcHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(RdbmsHandler.class);

  public RdbmsHandler(final MongoSinkTopicConfig config) {
    this(config, DEFAULT_OPERATIONS);
  }

  public RdbmsHandler(
      final MongoSinkTopicConfig config, final Map<OperationType, CdcOperation> operations) {
    super(config);
    registerOperations(operations);
  }

  @Override
  public Optional<WriteModel<BsonDocument>> handle(final SinkDocument doc) {
    BsonDocument keyDocument = doc.getKeyDoc().orElseGet(BsonDocument::new);
    BsonDocument valueDocument = doc.getValueDoc().orElseGet(BsonDocument::new);

    if (valueDocument.isEmpty()) {
      LOGGER.debug("skipping qlik tombstone event for kafka topic compaction");
      return Optional.empty();
    }

    OperationType operationType = OperationHelper.getOperationType(valueDocument);
    CdcOperation cdcOperation =
        getCdcOperation(operationType)
            .orElseThrow(
                () ->
                    new DataException(
                        format(
                            "Unable to determine the CDC operation from: %s",
                            valueDocument.toJson())));

    return Optional.ofNullable(cdcOperation.perform(new SinkDocument(keyDocument, valueDocument)));
  }
}
