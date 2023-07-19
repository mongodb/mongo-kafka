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
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SEPARATOR_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SUFFIX_CONFIG;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.bson.BsonDocument;
import org.bson.BsonString;

import com.mongodb.kafka.connect.source.MongoSourceConfig;

public class DefaultTopicMapperTest {
  private static final String PREFIX = "prefix";
  private static final String SUFFIX = "suffix";
  private static final String TOPIC_NAMESPACE_MAP_EXAMPLE1 =
      "{'myDb': 'topicTwo', 'myDb.myColl': 'topicOne'}";
  private static final String TOPIC_NAMESPACE_MAP_EXAMPLE2 =
      "{'*': 'topicThree', 'myDb.myColl': 'topicOne'}";

  @ParameterizedTest
  @ValueSource(strings = {"SEP", "-", "_", TOPIC_SEPARATOR_DEFAULT, "IMPLICIT_DEFAULT"})
  @DisplayName("test produces the expected topic")
  void testProducesTheExpectedTopic(final String topicSeparator) {
    boolean explicitTopicSep = !topicSeparator.equals("IMPLICIT_DEFAULT");
    String topicSep = explicitTopicSep ? topicSeparator : TOPIC_SEPARATOR_DEFAULT;
    Function<List<String>, TestTopicMapper> topicMapperCreator =
        configProperties -> {
          Map<String, String> configPropertiesMap = new HashMap<>();
          if (explicitTopicSep) {
            configPropertiesMap.put(TOPIC_SEPARATOR_CONFIG, topicSeparator);
          }
          assertEquals(0, configProperties.size() % 2);
          for (int i = 0; i < configProperties.size(); i += 2) {
            configPropertiesMap.put(configProperties.get(i), configProperties.get(i + 1));
          }
          return new TestTopicMapper(createMapper(createSourceConfig(configPropertiesMap)));
        };
    assertAll(
        () ->
            assertEquals(
                "", topicMapperCreator.apply(emptyList()).real().getTopic(new BsonDocument())),
        () ->
            assertEquals(
                "",
                topicMapperCreator
                    .apply(asList(TOPIC_NAMESPACE_MAP_CONFIG, "{}"))
                    .real()
                    .getTopic(new BsonDocument())),
        () -> assertEquals("myDb", topicMapperCreator.apply(emptyList()).getTopic("myDb")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "myDb"),
                topicMapperCreator.apply(asList(TOPIC_PREFIX_CONFIG, PREFIX)).getTopic("myDb")),
        () ->
            assertEquals(
                join(topicSep, "myDb", "suffix"),
                topicMapperCreator.apply(asList(TOPIC_SUFFIX_CONFIG, SUFFIX)).getTopic("myDb")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "myDb", "suffix"),
                topicMapperCreator
                    .apply(asList(TOPIC_PREFIX_CONFIG, PREFIX, TOPIC_SUFFIX_CONFIG, SUFFIX))
                    .getTopic("myDb")),
        () ->
            assertEquals(
                join(topicSep, "myDb", "myColl"),
                topicMapperCreator.apply(emptyList()).getTopic("myDb.myColl")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "myDb", "myColl"),
                topicMapperCreator
                    .apply(asList(TOPIC_PREFIX_CONFIG, PREFIX))
                    .getTopic("myDb.myColl")),
        () ->
            assertEquals(
                join(topicSep, "myDb", "myColl", "suffix"),
                topicMapperCreator
                    .apply(asList(TOPIC_SUFFIX_CONFIG, SUFFIX))
                    .getTopic("myDb.myColl")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "myDb", "myColl", "suffix"),
                topicMapperCreator
                    .apply(asList(TOPIC_PREFIX_CONFIG, PREFIX, TOPIC_SUFFIX_CONFIG, SUFFIX))
                    .getTopic("myDb.myColl")),
        () ->
            assertEquals(
                "topicTwo",
                topicMapperCreator
                    .apply(asList(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP_EXAMPLE1))
                    .getTopic("myDb")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "topicTwo", "suffix"),
                topicMapperCreator
                    .apply(
                        asList(
                            TOPIC_PREFIX_CONFIG,
                            PREFIX,
                            TOPIC_SUFFIX_CONFIG,
                            SUFFIX,
                            TOPIC_NAMESPACE_MAP_CONFIG,
                            TOPIC_NAMESPACE_MAP_EXAMPLE1))
                    .getTopic("myDb")),
        () ->
            assertEquals(
                "topicOne",
                topicMapperCreator
                    .apply(asList(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP_EXAMPLE1))
                    .getTopic("myDb.myColl")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "topicOne", "suffix"),
                topicMapperCreator
                    .apply(
                        asList(
                            TOPIC_PREFIX_CONFIG,
                            PREFIX,
                            TOPIC_SUFFIX_CONFIG,
                            SUFFIX,
                            TOPIC_NAMESPACE_MAP_CONFIG,
                            TOPIC_NAMESPACE_MAP_EXAMPLE1))
                    .getTopic("myDb.myColl")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "myDb2", "myColl2", "suffix"),
                topicMapperCreator
                    .apply(
                        asList(
                            TOPIC_PREFIX_CONFIG,
                            PREFIX,
                            TOPIC_SUFFIX_CONFIG,
                            SUFFIX,
                            TOPIC_NAMESPACE_MAP_CONFIG,
                            TOPIC_NAMESPACE_MAP_EXAMPLE1))
                    .getTopic("myDb2.myColl2")),
        () ->
            assertEquals(
                join(topicSep, "topicTwo", "myColl2"),
                topicMapperCreator
                    .apply(asList(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP_EXAMPLE1))
                    .getTopic("myDb.myColl2")),
        () ->
            assertEquals(
                join(topicSep, "prefix", "topicTwo", "myColl2", "suffix"),
                topicMapperCreator
                    .apply(
                        asList(
                            TOPIC_PREFIX_CONFIG,
                            PREFIX,
                            TOPIC_SUFFIX_CONFIG,
                            SUFFIX,
                            TOPIC_NAMESPACE_MAP_CONFIG,
                            TOPIC_NAMESPACE_MAP_EXAMPLE1))
                    .getTopic("myDb.myColl2")),
        () ->
            assertEquals(
                "topicThree",
                topicMapperCreator
                    .apply(asList(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP_EXAMPLE2))
                    .getTopic("myDb2.myColl2")),
        () ->
            assertEquals(
                "topicOne",
                topicMapperCreator
                    .apply(asList(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP_EXAMPLE2))
                    .getTopic("myDb.myColl")));
  }

  @Test
  @DisplayName("test throws configuration exceptions for invalid maps")
  void testThrowConfigurationExceptionsForInvalidMappings() {
    assertAll(
        "Invalid configuration mappings",
        () -> assertThrows(ConfigException.class, () -> createMapper("[]")),
        () -> assertThrows(ConfigException.class, () -> createMapper("{'myDb.myColl': 1234}")));
  }

  private static DefaultTopicMapper createMapper(final MongoSourceConfig config) {
    DefaultTopicMapper topicMapper = new DefaultTopicMapper();
    topicMapper.configure(config);
    return topicMapper;
  }

  private static void createMapper(final String topicNamespaceMap) {
    Map<String, String> configPropertiesMap = new HashMap<>();
    configPropertiesMap.put(TOPIC_NAMESPACE_MAP_CONFIG, topicNamespaceMap);
    createMapper(createSourceConfig(configPropertiesMap));
  }

  /**
   * Unlike {@link DefaultTopicMapper#getTopic(BsonDocument)}, this class enables one to use a
   * namespace directly via {@link #getTopic(String)} instead of creating a change stream document.
   */
  private static final class TestTopicMapper {
    private final DefaultTopicMapper real;

    TestTopicMapper(final DefaultTopicMapper mapper) {
      real = mapper;
    }

    String getTopic(final String namespace) {
      int separatorIdx = namespace.indexOf('.');
      String dbName;
      String collName;
      if (separatorIdx == -1) {
        dbName = namespace;
        collName = null;
      } else {
        dbName = namespace.substring(0, separatorIdx);
        collName = namespace.substring(separatorIdx + 1);
      }
      BsonDocument ns = new BsonDocument("db", new BsonString(dbName));
      if (collName != null) {
        ns.append("coll", new BsonString(collName));
      }
      return real.getTopic(new BsonDocument("ns", ns));
    }

    DefaultTopicMapper real() {
      return real;
    }
  }
}
