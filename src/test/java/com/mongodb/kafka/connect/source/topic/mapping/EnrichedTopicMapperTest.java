package com.mongodb.kafka.connect.source.topic.mapping;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.*;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.BsonDocument;

public class EnrichedTopicMapperTest extends AbstractTopicMapperTest {

  private static final BsonDocument NAMESPACE_REGEX_MATCHING_DOCUMENT =
      BsonDocument.parse("{ns: {db: 'db-1', coll: 'coll-1'}}");

  private static final String TOPIC_NAMESPACE_REGEX_MAP =
      "{\"/db-.*\\\\.coll-.*/\": \"regexTopic\", \"db1.coll1\": \"mappedDBAndCollTopic\", \"db1\": \"mappedDBTopic\", \"*\": \"allTopic\"}";

  private static final String TOPIC_NAMESPACE_REGEX_DB_MAP =
      "{\"/db-.*\\\\.coll-.*/\": \"regexTopic\", \"db1\": \"mappedDBTopic\", \"*\": \"allTopic\"}";

  private static final String TOPIC_NAMESPACE_ALT_REGEX_MAP =
      "{\"/db-.*\\\\.coll-.*/\": \"regexTopic\", \"db-1.coll-1\": \"mappedDBAndCollTopic\"}";

  private static final String TOPIC_NAMESPACE_ALT_2_REGEX_MAP =
      "{\"/db-.*\\\\.coll-.*/\": \"regexTopic\", \"db-1\": \"mappedDBTopic\"}";

  private static final String TOPIC_NAMESPACE_ALT_3_REGEX_MAP =
      "{\"/db-.*/\": \"regexTopic."
          + TOPIC_NAMESPACE_COLLECTION_RESERVED_WORD
          + "\", \"*\": \"allTopic\"}";

  @Test
  @DisplayName("test produces the expected topics from regular expressions")
  void testProducesRegexExpectedTopic() {
    assertAll(
        () ->
            assertEquals(
                "regexTopic",
                createMapper(
                        createSourceConfig(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_REGEX_MAP))
                    .getTopic(NAMESPACE_REGEX_MATCHING_DOCUMENT)),
        () ->
            assertEquals(
                "mappedDBAndCollTopic",
                createMapper(
                        createSourceConfig(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_REGEX_MAP))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "mappedDBTopic.coll1",
                createMapper(
                        createSourceConfig(
                            TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_REGEX_DB_MAP))
                    .getTopic(NAMESPACE_DOCUMENT)),
        () ->
            assertEquals(
                "allTopic",
                createMapper(
                        createSourceConfig(TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_REGEX_MAP))
                    .getTopic(NAMESPACE_ALT_DATABASE_DOCUMENT)),
        () ->
            assertEquals(
                "mappedDBAndCollTopic",
                createMapper(
                        createSourceConfig(
                            TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_ALT_REGEX_MAP))
                    .getTopic(NAMESPACE_REGEX_MATCHING_DOCUMENT)),
        () ->
            assertEquals(
                "mappedDBTopic.coll-1",
                createMapper(
                        createSourceConfig(
                            TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_ALT_2_REGEX_MAP))
                    .getTopic(NAMESPACE_REGEX_MATCHING_DOCUMENT)),
        () ->
            assertEquals(
                "regexTopic.coll-1",
                createMapper(
                        createSourceConfig(
                            TOPIC_NAMESPACE_MAP_CONFIG, TOPIC_NAMESPACE_ALT_3_REGEX_MAP))
                    .getTopic(NAMESPACE_REGEX_MATCHING_DOCUMENT)));
  }

  @Test
  @DisplayName(
      "test throws configuration exceptions for invalid maps containing regular expressions")
  void testThrowConfigurationExceptionsForInvalidRegexMappings() {
    assertAll(
        "Invalid regular expressions configuration mappings",
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': \"{'/db-.*\\\\.coll-.*': 'myTopic'}\"}",
                                TOPIC_NAMESPACE_MAP_CONFIG)))),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': \"{'db-.*\\\\.coll-.*/': 'myTopic'}\"}",
                                TOPIC_NAMESPACE_MAP_CONFIG)))),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    createMapper(
                        createSourceConfig(
                            format(
                                "{'%s': \"{'/db-.**\\\\.coll-.**/': 'myTopic'}\"}",
                                TOPIC_NAMESPACE_MAP_CONFIG)))));
  }

  @Override
  protected TopicMapper getTopicMapper() {
    return new EnrichedTopicMapper();
  }
}
