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
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SEPARATOR_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SUFFIX_CONFIG;
import static com.mongodb.kafka.connect.util.BsonDocumentFieldLookup.fieldLookup;
import static com.mongodb.kafka.connect.util.ConfigHelper.documentFromString;
import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.util.ConnectConfigException;

public class DefaultTopicMapper implements TopicMapper {

  private static final String DB_FIELD_PATH = "ns.db";
  private static final String COLL_FIELD_PATH = "ns.coll";
  private static final String ALL = "*";
  private static final char NAMESPACE_SEPARATOR = '.';

  private String separator;
  private String prefix;
  private String suffix;
  private Document topicNamespaceMap;
  private Map<String, String> namespaceTopicCache;

  @Override
  public void configure(final MongoSourceConfig config) {
    String prefix = config.getString(TOPIC_PREFIX_CONFIG);
    String suffix = config.getString(TOPIC_SUFFIX_CONFIG);

    this.separator = config.getString(TOPIC_SEPARATOR_CONFIG);
    this.prefix = prefix.isEmpty() ? prefix : prefix + separator;
    this.suffix = suffix.isEmpty() ? suffix : separator + suffix;
    this.topicNamespaceMap =
        documentFromString(config.getString(TOPIC_NAMESPACE_MAP_CONFIG)).orElse(new Document());

    if (topicNamespaceMap.values().stream().anyMatch(i -> !(i instanceof String))) {
      throw new ConnectConfigException(
          TOPIC_NAMESPACE_MAP_CONFIG,
          config.getString(TOPIC_NAMESPACE_MAP_CONFIG),
          format("All values of `%s` must be strings", TOPIC_NAMESPACE_MAP_CONFIG));
    }

    this.namespaceTopicCache = new HashMap<>();
  }

  @Override
  public String getTopic(final BsonDocument changeStreamDocument) {

    String dbName = getStringFromPath(DB_FIELD_PATH, changeStreamDocument);
    if (dbName.isEmpty()) {
      return dbName;
    }
    String collName = getStringFromPath(COLL_FIELD_PATH, changeStreamDocument);
    String namespace = namespace(dbName, collName);

    String cachedTopic = namespaceTopicCache.get(namespace);
    if (cachedTopic == null) {
      cachedTopic = decorateTopicName(getUndecoratedTopicNameFromNamespaceMap(dbName, collName));
      namespaceTopicCache.put(namespace, cachedTopic);
    }
    return cachedTopic;
  }

  private String getStringFromPath(
      final String fieldPath, final BsonDocument changeStreamDocument) {
    return fieldLookup(fieldPath, changeStreamDocument)
        .map(bsonValue -> bsonValue.isString() ? bsonValue.asString().getValue() : "")
        .orElse("");
  }

  /*
   * Checks the mapping in the following order for the topic name to use:
   *
   * Exact match: namespace (Either: dbName.collName or dbName)
   * Partial match: dbName
   * Wildcard match: *
   */
  private String getUndecoratedTopicNameFromNamespaceMap(
      final String dbName, final String collName) {
    String exactMatch = topicNamespaceMap.get(namespace(dbName, collName), "");
    if (!exactMatch.isEmpty()) {
      return exactMatch;
    }

    String databaseMatch = topicNamespaceMap.get(dbName, "");
    if (!databaseMatch.isEmpty()) {
      return undecoratedTopicName(databaseMatch, collName);
    }

    return topicNamespaceMap.get(ALL, undecoratedTopicName(dbName, collName));
  }

  private static String namespace(final String dbName, final String collName) {
    return collName.isEmpty() ? dbName : dbName + NAMESPACE_SEPARATOR + collName;
  }

  private String undecoratedTopicName(
      final String dbNameOrMappedTopicNamePart, final String collName) {
    return collName.isEmpty()
        ? dbNameOrMappedTopicNamePart
        : dbNameOrMappedTopicNamePart + separator + collName;
  }

  private String decorateTopicName(final String undecoratedTopicName) {
    return prefix + undecoratedTopicName + suffix;
  }
}
