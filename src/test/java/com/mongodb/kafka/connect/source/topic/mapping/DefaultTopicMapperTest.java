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
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.source.MongoSourceConfig;

@RunWith(JUnitPlatform.class)
public class DefaultTopicMapperTest {

  private static final String PREFIX = "prefix";
  private static final String SUFFIX = "suffix";
  private static final BsonDocument DB_ONLY_NAMESPACE_DOCUMENT =
      BsonDocument.parse("{ns: {db: 'db1'}}");
  private static final BsonDocument NAMESPACE_DOCUMENT =
      BsonDocument.parse("{ns: {db: 'db1', coll: 'coll1'}}");

  private static final BsonDocument NAMESPACE_ALT_DATABASE_DOCUMENT =
      BsonDocument.parse("{ns: {db: 'db2', coll: 'coll2'}}");

  private static final BsonDocument NAMESPACE_ALT_COLLECTION_DOCUMENT =
      BsonDocument.parse("{ns: {db: 'db1', coll: 'coll2'}}");

  private static final String TOPIC_NAMESPACE_MAP =
      "{\"db1\": \"mappedDBTopic\", \"db1.coll1\": \"mappedDBAndCollTopic\"}";

  private static final String TOPIC_NAMESPACE_ALL_MAP =
      "{\"*\": \"allTopic\", \"db2.coll2\": \"allExceptionTopic\"}";

  @Test
  @DisplayName("test produces the expected topic")
  void testProducesTheExpectedTopic() {
    assertAll(
        () -> assertEquals("", createMapper(createSourceConfig()).getTopic(new BsonDocument())),
        () ->
            assertEquals(
                "",
                createMapper(createSourceConfig(format("{'%s': '{}'}", TOPIC_NAMESPACE_MAP_CONFIG)))
                    .getTopic(new BsonDocument())),
        () ->
            assertEquals(
                "db1", createMapper(createSourceConfig()).getTopic(DB_ONLY_NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.db1",
                createMapper(createSourceConfig(TOPIC_PREFIX_CONFIG, PREFIX))
                    .getTopic(DB_ONLY_NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "db1.suffix",
                createMapper(createSourceConfig(TOPIC_SUFFIX_CONFIG, SUFFIX))
                    .getTopic(DB_ONLY_NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.db1.suffix",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s', '%s': '%s'}",
                                TOPIC_PREFIX_CONFIG, PREFIX, TOPIC_SUFFIX_CONFIG, SUFFIX)))
                    .getTopic(DB_ONLY_NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "db1.coll1", createMapper(createSourceConfig()).getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.db1.coll1",
                createMapper(createSourceConfig(TOPIC_PREFIX_CONFIG, PREFIX))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "db1.coll1.suffix",
                createMapper(createSourceConfig(TOPIC_SUFFIX_CONFIG, SUFFIX))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.db1.coll1.suffix",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s', '%s': '%s'}",
                                TOPIC_PREFIX_CONFIG, PREFIX, TOPIC_SUFFIX_CONFIG, SUFFIX)))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "mappedDBTopic",
                createMapper(createSourceConfig(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP))
                    .getTopic(DB_ONLY_NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.mappedDBTopic.suffix",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s', '%s': '%s', '%s': '%s'}",
                                TOPIC_PREFIX_CONFIG,
                                PREFIX,
                                TOPIC_SUFFIX_CONFIG,
                                SUFFIX,
                                TOPIC_NAMESPACE_MAP_CONFIG,
                                TOPIC_NAMESPACE_MAP)))
                    .getTopic(DB_ONLY_NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "mappedDBAndCollTopic",
                createMapper(createSourceConfig(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.mappedDBAndCollTopic.suffix",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s', '%s': '%s', '%s': '%s'}",
                                TOPIC_PREFIX_CONFIG,
                                PREFIX,
                                TOPIC_SUFFIX_CONFIG,
                                SUFFIX,
                                TOPIC_NAMESPACE_MAP_CONFIG,
                                TOPIC_NAMESPACE_MAP)))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.db2.coll2.suffix",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s', '%s': '%s', '%s': '%s'}",
                                TOPIC_PREFIX_CONFIG,
                                PREFIX,
                                TOPIC_SUFFIX_CONFIG,
                                SUFFIX,
                                TOPIC_NAMESPACE_MAP_CONFIG,
                                TOPIC_NAMESPACE_MAP)))
                    .getTopic(NAMESPACE_ALT_DATABASE_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.mappedDBTopic.suffix",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s', '%s': '%s', '%s': '%s'}",
                                TOPIC_PREFIX_CONFIG,
                                PREFIX,
                                TOPIC_SUFFIX_CONFIG,
                                SUFFIX,
                                TOPIC_NAMESPACE_MAP_CONFIG,
                                TOPIC_NAMESPACE_MAP)))
                    .getTopic(DB_ONLY_NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "mappedDBTopic.coll2",
                createMapper(createSourceConfig(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_MAP))
                    .getTopic(NAMESPACE_ALT_COLLECTION_DOCUMENT)),
        () ->
            assertEquals(
                "prefix.mappedDBTopic.coll2.suffix",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s', '%s': '%s', '%s': '%s'}",
                                TOPIC_PREFIX_CONFIG,
                                PREFIX,
                                TOPIC_SUFFIX_CONFIG,
                                SUFFIX,
                                TOPIC_NAMESPACE_MAP_CONFIG,
                                TOPIC_NAMESPACE_MAP)))
                    .getTopic(NAMESPACE_ALT_COLLECTION_DOCUMENT)),
        () ->
            assertEquals(
                "allTopic",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s'}",
                                TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_ALL_MAP)))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "allExceptionTopic",
                createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': '%s'}",
                                TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_ALL_MAP)))
                    .getTopic(NAMESPACE_ALT_DATABASE_DOCUMENT)));
  }

  @Test
  @DisplayName("test throws configuration exceptions for invalid maps")
  void testThrowConfigurationExceptionsForInvalidMappings() {
    assertAll(
        "Invalid configuration mappings",
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    createMapper(
                        createSourceConfig(format("{'%s': '[]'}", TOPIC_NAMESPACE_MAP_CONFIG)))),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    createMapper(
                        createSourceConfig(
                            format("{'%s': \"{'db.coll': 1234}\"}", TOPIC_NAMESPACE_MAP_CONFIG)))));
  }

  private TopicMapper createMapper(final MongoSourceConfig config) {
    TopicMapper topicMapper = new DefaultTopicMapper();
    topicMapper.configure(config);
    return topicMapper;
  }
}
