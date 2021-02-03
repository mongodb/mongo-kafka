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

package com.mongodb.kafka.connect.sink.namespace.mapping;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.util.BsonDocumentFieldLookup.fieldLookup;
import static java.lang.String.format;

import java.util.Optional;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.util.ConnectConfigException;

public class FieldPathNamespaceMapper implements NamespaceMapper {

  private String defaultDatabaseName;
  private String defaultCollectionName;
  private String keyDatabaseFieldPath;
  private String valueDatabaseFieldPath;
  private String keyCollectionFieldPath;
  private String valueCollectionFieldPath;
  private boolean throwErrorIfInvalid;

  @Override
  public void configure(final MongoSinkTopicConfig config) {
    this.defaultDatabaseName = config.getString(DATABASE_CONFIG);
    this.defaultCollectionName = config.getString(COLLECTION_CONFIG);
    if (this.defaultCollectionName.isEmpty()) {
      this.defaultCollectionName = config.getTopic();
    }

    this.throwErrorIfInvalid = config.getBoolean(FIELD_NAMESPACE_MAPPER_ERROR_IF_INVALID_CONFIG);
    this.keyDatabaseFieldPath = config.getString(FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG);
    this.valueDatabaseFieldPath = config.getString(FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG);
    this.keyCollectionFieldPath = config.getString(FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG);
    this.valueCollectionFieldPath =
        config.getString(FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG);

    if (keyDatabaseFieldPath.isEmpty()
        && valueDatabaseFieldPath.isEmpty()
        && keyCollectionFieldPath.isEmpty()
        && valueCollectionFieldPath.isEmpty()) {
      throw new ConnectConfigException(
          NAMESPACE_MAPPER_CONFIG,
          config.getString(NAMESPACE_MAPPER_CONFIG),
          "Missing configuration for the FieldBasedNamespaceMapper. "
              + "Please configure the database and / or collection field path.");
    }

    if (!keyDatabaseFieldPath.isEmpty() && !valueDatabaseFieldPath.isEmpty()) {
      throw new ConnectConfigException(
          FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
          keyDatabaseFieldPath,
          format(
              "Cannot set both: '%s' and '%s'",
              FIELD_KEY_DATABASE_NAMESPACE_MAPPER_CONFIG,
              FIELD_VALUE_DATABASE_NAMESPACE_MAPPER_CONFIG));
    } else if (!keyCollectionFieldPath.isEmpty() && !valueCollectionFieldPath.isEmpty()) {
      throw new ConnectConfigException(
          FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG,
          keyCollectionFieldPath,
          format(
              "Cannot set both: '%s' and '%s'",
              FIELD_KEY_COLLECTION_NAMESPACE_MAPPER_CONFIG,
              FIELD_VALUE_COLLECTION_NAMESPACE_MAPPER_CONFIG));
    }
  }

  @Override
  public MongoNamespace getNamespace(final SinkRecord sinkRecord, final SinkDocument sinkDocument) {
    String databaseName = defaultDatabaseName;
    String collectionName = defaultCollectionName;

    if (!keyDatabaseFieldPath.isEmpty()) {
      databaseName = pathLookup(sinkRecord, sinkDocument, keyDatabaseFieldPath, true, databaseName);
    } else if (!valueDatabaseFieldPath.isEmpty()) {
      databaseName =
          pathLookup(sinkRecord, sinkDocument, valueDatabaseFieldPath, false, databaseName);
    }

    if (!keyCollectionFieldPath.isEmpty()) {
      collectionName =
          pathLookup(sinkRecord, sinkDocument, keyCollectionFieldPath, true, collectionName);
    } else if (!valueCollectionFieldPath.isEmpty()) {
      collectionName =
          pathLookup(sinkRecord, sinkDocument, valueCollectionFieldPath, false, collectionName);
    }

    return new MongoNamespace(databaseName, collectionName);
  }

  private String pathLookup(
      final SinkRecord sinkRecord,
      final SinkDocument sinkDocument,
      final String path,
      final boolean isKey,
      final String defaultValue) {
    Optional<BsonDocument> optionalData =
        isKey ? sinkDocument.getKeyDoc() : sinkDocument.getValueDoc();

    if (continueProcessing(optionalData.isPresent())) {
      BsonDocument data =
          optionalData.orElseThrow(
              () ->
                  new DataException(
                      format("Invalid %s document: %s", isKey ? "key" : "value", sinkRecord)));

      Optional<BsonValue> optionalFieldValue = fieldLookup(path, data);
      if (continueProcessing(optionalFieldValue.isPresent())) {
        BsonValue fieldValue =
            optionalFieldValue.orElseThrow(
                () ->
                    new DataException(
                        format("Missing document path '%s': %s", path, data.toJson())));

        if (continueProcessing(fieldValue.isString())) {
          if (!fieldValue.isString()) {
            throw new DataException(
                format(
                    "Invalid type for %s field path '%s', expected a String: %s",
                    fieldValue.getBsonType(), path, data.toJson()));
          }

          return fieldValue.asString().getValue();
        }
      }
    }
    return defaultValue;
  }

  private boolean continueProcessing(final boolean canContinue) {
    return canContinue || throwErrorIfInvalid;
  }
}
