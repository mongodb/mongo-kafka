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

package com.mongodb.kafka.connect.source.topic.mapping;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_NAMESPACE_MAP_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SUFFIX_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.documentFromString;
import static java.lang.String.format;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import com.mongodb.kafka.connect.source.MongoSourceConfig;

public class DefaultTopicMapper implements TopicMapper {

  private static final String NS_KEY = "ns";
  private static final String DB_KEY = "db";
  private static final String COLL_KEY = "coll";
  private static final String ALL = "*";

  private String prefix;
  private String suffix;
  private Document topicNamespaceMap;

  @Override
  public void configure(final MongoSourceConfig config) {
    String prefix = config.getString(TOPIC_PREFIX_CONFIG).trim();
    String suffix = config.getString(TOPIC_SUFFIX_CONFIG).trim();

    this.prefix = prefix.isEmpty() ? "" : format("%s.", prefix);
    this.suffix = suffix.isEmpty() ? "" : format(".%s", suffix);
    this.topicNamespaceMap =
        documentFromString(config.getString(TOPIC_NAMESPACE_MAP_CONFIG)).orElse(new Document());
  }

  @Override
  public String getTopic(final BsonDocument changeStreamDocument) {
    BsonDocument namespaceDocument = changeStreamDocument.getDocument(NS_KEY, new BsonDocument());
    String dbName = namespaceDocument.getString(DB_KEY, new BsonString("")).getValue();
    String collName = namespaceDocument.getString(COLL_KEY, new BsonString("")).getValue();
    String topicName = collName.isEmpty() ? dbName : format("%s.%s", dbName, collName);

    if (topicNamespaceMap.containsKey(topicName)) {
      topicName = topicNamespaceMap.get(topicName).toString();
    } else if (topicNamespaceMap.containsKey(dbName) && !collName.isEmpty()) {
      topicName = format("%s.%s", topicNamespaceMap.get(dbName), collName);
    } else if (topicNamespaceMap.containsKey(ALL)) {
      topicName = topicNamespaceMap.getString(ALL);
    }

    return format("%s%s%s", prefix, topicName, suffix);
  }
}
