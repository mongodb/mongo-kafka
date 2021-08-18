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

import static com.mongodb.kafka.connect.source.MongoSourceConfig.*;
import static java.lang.String.format;

import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.util.ConnectConfigException;

public class EnrichedTopicMapper extends AbstractTopicMapper {

  private static final String REGEX_DELIMITER = "/";

  @Override
  public void configure(final MongoSourceConfig config) {
    super.configure(config);

    if (topicNamespaceMap.keySet().stream()
        .anyMatch(
            k ->
                (k.startsWith(REGEX_DELIMITER) && !k.endsWith(REGEX_DELIMITER))
                    || (k.endsWith(REGEX_DELIMITER) && !k.startsWith(REGEX_DELIMITER)))) {
      throw new ConnectConfigException(
          TOPIC_NAMESPACE_MAP_CONFIG,
          config.getString(TOPIC_NAMESPACE_MAP_CONFIG),
          format(
              "All regular expressions of `%s` must begin AND end with `%s`",
              TOPIC_NAMESPACE_MAP_CONFIG, REGEX_DELIMITER));
    }

    if (topicNamespaceMap.keySet().stream()
        .filter(this::isRegexKey)
        .anyMatch(k -> !isValidRegexKey(k))) {
      throw new ConnectConfigException(
          TOPIC_NAMESPACE_MAP_CONFIG,
          config.getString(TOPIC_NAMESPACE_MAP_CONFIG),
          format("All regular expressions of `%s` must be valid", TOPIC_NAMESPACE_MAP_CONFIG));
    }
  }

  @Override
  protected String getAltTopicNameFromNamespaceMap(
      final String namespace, final String dbName, final String collName) {
    String regexMatch = getTopicNameFromRegex(namespace, dbName, collName);
    if (!regexMatch.isEmpty()) {
      return regexMatch;
    }

    return topicNamespaceMap.get(ALL, namespace);
  }

  private String getTopicNameFromRegex(
      final String namespace, final String dbName, final String collName) {
    Optional<String> optKey =
        topicNamespaceMap.keySet().stream()
            .filter(this::isRegexKey)
            .filter(k -> Pattern.compile(getPatternFromKey(k)).asPredicate().test(namespace))
            .findFirst();

    return optKey
        .map(
            s ->
                topicNamespaceMap
                    .get(s, "")
                    .replace(TOPIC_NAMESPACE_DATABASE_RESERVED_WORD, dbName)
                    .replace(TOPIC_NAMESPACE_COLLECTION_RESERVED_WORD, collName))
        .orElse("");
  }

  private boolean isRegexKey(String key) {
    return key.startsWith(REGEX_DELIMITER) && key.endsWith(REGEX_DELIMITER);
  }

  private String getPatternFromKey(String key) {
    assert isRegexKey(key);
    return key.substring(1, key.length() - 1);
  }

  private boolean isValidRegexKey(String key) {
    if (!isRegexKey(key)) {
      return false;
    }
    try {
      Pattern.compile(getPatternFromKey(key));
      return true;
    } catch (PatternSyntaxException e) {
      return false;
    }
  }
}
