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
package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.util.TimeseriesValidation.validateCollection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;

final class StartedMongoSinkTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkTask.class);
  private static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions();

  private final MongoSinkConfig sinkConfig;
  private final MongoClient mongoClient;
  private final Consumer<MongoProcessedSinkRecordData> errorReporter;
  private final Set<MongoNamespace> checkedTimeseriesNamespaces;

  StartedMongoSinkTask(
      final MongoSinkConfig sinkConfig,
      final MongoClient mongoClient,
      final Consumer<MongoProcessedSinkRecordData> errorReporter) {
    this.sinkConfig = sinkConfig;
    this.mongoClient = mongoClient;
    this.errorReporter = errorReporter;
    checkedTimeseriesNamespaces = new HashSet<>();
  }

  /** @see MongoSinkTask#stop() */
  void stop() {
    mongoClient.close();
  }

  /** @see MongoSinkTask#put(Collection) */
  void put(final Collection<SinkRecord> records) {
    if (records.isEmpty()) {
      LOGGER.debug("No sink records to process for current poll operation");
      return;
    }
    MongoSinkRecordProcessor.orderedGroupByTopicAndNamespace(records, sinkConfig, errorReporter)
        .forEach(this::bulkWriteBatch);
  }

  private void bulkWriteBatch(final List<MongoProcessedSinkRecordData> batch) {
    if (batch.isEmpty()) {
      return;
    }

    MongoNamespace namespace = batch.get(0).getNamespace();
    MongoSinkTopicConfig config = batch.get(0).getConfig();
    checkTimeseries(namespace, config);

    List<WriteModel<BsonDocument>> writeModels =
        batch.stream()
            .map(MongoProcessedSinkRecordData::getWriteModel)
            .collect(Collectors.toList());

    try {
      LOGGER.debug(
          "Bulk writing {} document(s) into collection [{}]",
          writeModels.size(),
          namespace.getFullName());
      BulkWriteResult result =
          mongoClient
              .getDatabase(namespace.getDatabaseName())
              .getCollection(namespace.getCollectionName(), BsonDocument.class)
              .bulkWrite(writeModels, BULK_WRITE_OPTIONS);
      LOGGER.debug("Mongodb bulk write result: {}", result);
      checkRateLimit(config);
    } catch (MongoException e) {
      LOGGER.warn(
          "Writing {} document(s) into collection [{}] failed.",
          writeModels.size(),
          namespace.getFullName());
      handleMongoException(config, writeModels, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new DataException("Rate limiting was interrupted", e);
    } catch (Exception e) {
      if (config.logErrors()) {
        LOGGER.error("Failed to write mongodb documents", e);
      }
      if (!config.tolerateErrors()) {
        throw new DataException("Failed to write mongodb documents", e);
      }
    }
  }

  private void checkTimeseries(final MongoNamespace namespace, final MongoSinkTopicConfig config) {
    if (!checkedTimeseriesNamespaces.contains(namespace)) {
      if (config.isTimeseries()) {
        validateCollection(mongoClient, namespace, config);
      }
      checkedTimeseriesNamespaces.add(namespace);
    }
  }

  private static void checkRateLimit(final MongoSinkTopicConfig config)
      throws InterruptedException {
    RateLimitSettings rls = config.getRateLimitSettings();

    if (rls.isTriggered()) {
      LOGGER.debug(
          "Rate limit settings triggering {}ms defer timeout after processing {}"
              + " further batches for topic {}",
          rls.getTimeoutMs(),
          rls.getEveryN(),
          config.getTopic());
      Thread.sleep(rls.getTimeoutMs());
    }
  }

  private static void handleMongoException(
      final MongoSinkTopicConfig config,
      final List<WriteModel<BsonDocument>> writeModels,
      final MongoException e) {
    if (config.logErrors()) {
      LOGGER.error("Error on mongodb operation", e);
      if (e instanceof MongoBulkWriteException) {
        LOGGER.error("Mongodb bulk write (partially) failed", e);
        LOGGER.error("WriteResult: {}", ((MongoBulkWriteException) e).getWriteResult());
        LOGGER.error(
            "WriteErrors: {}",
            generateWriteErrors(((MongoBulkWriteException) e).getWriteErrors(), writeModels));
        LOGGER.error("WriteConcernError: {}", ((MongoBulkWriteException) e).getWriteConcernError());
      }
    }
    if (!config.tolerateErrors()) {
      throw new DataException("Failed to write mongodb documents", e);
    }
  }

  private static String generateWriteErrors(
      final List<BulkWriteError> bulkWriteErrorList,
      final List<WriteModel<BsonDocument>> writeModels) {
    List<String> errorString = new ArrayList<>();
    for (final BulkWriteError bulkWriteError : bulkWriteErrorList) {
      if (bulkWriteError.getIndex() < writeModels.size()) {
        errorString.add(
            "BulkWriteError{"
                + "writeModel="
                + writeModels.get(bulkWriteError.getIndex())
                + ", code="
                + bulkWriteError.getCode()
                + ", message='"
                + bulkWriteError.getMessage()
                + '\''
                + ", details="
                + bulkWriteError.getDetails()
                + '}');
      } else {
        errorString.add(bulkWriteError.toString());
      }
    }
    return "[" + String.join(", ", errorString) + "]";
  }
}
