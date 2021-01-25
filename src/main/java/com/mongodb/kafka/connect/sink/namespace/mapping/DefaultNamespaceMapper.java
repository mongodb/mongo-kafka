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

import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class DefaultNamespaceMapper implements NamespaceMapper {

  private MongoNamespace namespace;

  @Override
  public void configure(final MongoSinkTopicConfig config) {
    String databaseName = config.getString(DATABASE_CONFIG);
    String collectionName = config.getString(COLLECTION_CONFIG);
    if (collectionName.isEmpty()) {
      collectionName = config.getTopic();
    }
    this.namespace = new MongoNamespace(databaseName, collectionName);
  }

  @Override
  public MongoNamespace getNamespace(final SinkRecord sinkRecord, final SinkDocument sinkDocument) {
    return namespace;
  }
}
