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

package com.mongodb.kafka.connect.sink.writemodel.strategy;

import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonDocument;

import com.mongodb.client.model.WriteModel;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public final class WriteModelStrategyHelper {

  public static WriteModel<BsonDocument> createWriteModel(
      final MongoSinkTopicConfig config, final SinkDocument document) {
    if (document.getValueDoc().isPresent()) {
      return createValueWriteModel(config, document);
    } else if (document.getKeyDoc().isPresent()) {
      return createKeyDeleteOneModel(config, document);
    } else {
      throw new DataException("Invalid Sink Record neither key doc nor value doc were present");
    }
  }

  static WriteModel<BsonDocument> createValueWriteModel(
      final MongoSinkTopicConfig config, final SinkDocument sinkDocument) {
    try {
      return config.getWriteModelStrategy().createWriteModel(sinkDocument);
    } catch (Exception e) {
      if (e instanceof DataException) {
        throw e;
      }
      throw new DataException("Could not build the WriteModel.", e);
    }
  }

  static WriteModel<BsonDocument> createKeyDeleteOneModel(
      final MongoSinkTopicConfig config, final SinkDocument sinkDocument) {
    try {
      return config
          .getDeleteOneWriteModelStrategy()
          .map(s -> s.createWriteModel(sinkDocument))
          .orElseThrow(() -> new DataException("Could not create write model"));
    } catch (Exception e) {
      throw new DataException("Could not create write model", e);
    }
  }

  private WriteModelStrategyHelper() {}
}
