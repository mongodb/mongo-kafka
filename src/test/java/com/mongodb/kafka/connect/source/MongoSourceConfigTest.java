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

package com.mongodb.kafka.connect.source;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLATION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_TOLERANCE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.FULL_DOCUMENT_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_KEY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_INFER_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SUFFIX_CONFIG;
import static com.mongodb.kafka.connect.source.SourceTestHelper.CLIENT_URI_AUTH_SETTINGS;
import static com.mongodb.kafka.connect.source.SourceTestHelper.CLIENT_URI_DEFAULT_SETTINGS;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.bson.Document;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.changestream.FullDocument;

import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;
import com.mongodb.kafka.connect.source.topic.mapping.DefaultTopicMapper;
import com.mongodb.kafka.connect.source.topic.mapping.TestTopicMapper;

import com.github.jcustenborder.kafka.connect.utils.config.MarkdownFormatter;

class MongoSourceConfigTest {

  @Test
  @DisplayName("build config doc (no test)")
  // CHECKSTYLE:OFF
  void doc() {
    System.out.println(MongoSourceConfig.CONFIG.toRst());
    System.out.println(MarkdownFormatter.toMarkdown(MongoSourceConfig.CONFIG));
    assertTrue(true);
  }
  // CHECKSTYLE:ON

  @Test
  @DisplayName("test client uri")
  void testClientUri() {
    assertAll(
        "Client uri",
        () ->
            assertEquals(
                CLIENT_URI_DEFAULT_SETTINGS, createSourceConfig().getConnectionString().toString()),
        () ->
            assertEquals(
                CLIENT_URI_AUTH_SETTINGS,
                createSourceConfig(CONNECTION_URI_CONFIG, CLIENT_URI_AUTH_SETTINGS)
                    .getConnectionString()
                    .toString()),
        () -> assertInvalid(CONNECTION_URI_CONFIG, "invalid connection string"));
  }

  @Test
  @DisplayName("test output format")
  void testOutputFormat() {
    assertAll(
        "Output format",
        () -> assertEquals(OutputFormat.JSON, createSourceConfig().getKeyOutputFormat()),
        () -> assertEquals(OutputFormat.JSON, createSourceConfig().getValueOutputFormat()),
        () ->
            assertEquals(
                OutputFormat.BSON,
                createSourceConfig(OUTPUT_FORMAT_KEY_CONFIG, "bson").getKeyOutputFormat()),
        () ->
            assertEquals(
                OutputFormat.BSON,
                createSourceConfig(OUTPUT_FORMAT_VALUE_CONFIG, "bson").getValueOutputFormat()),
        () ->
            assertEquals(
                OutputFormat.SCHEMA,
                createSourceConfig(OUTPUT_FORMAT_KEY_CONFIG, "schema").getKeyOutputFormat()),
        () ->
            assertEquals(
                OutputFormat.SCHEMA,
                createSourceConfig(OUTPUT_FORMAT_VALUE_CONFIG, "schema").getValueOutputFormat()),
        () -> assertInvalid(OUTPUT_FORMAT_KEY_CONFIG, "avro"),
        () -> assertInvalid(OUTPUT_FORMAT_VALUE_CONFIG, "avro"),
        () -> assertInvalid(OUTPUT_FORMAT_KEY_CONFIG, "[]"),
        () -> assertInvalid(OUTPUT_FORMAT_VALUE_CONFIG, "[]"));
  }

  @Test
  @DisplayName("test output schema infer value")
  void testOutputSchemaInferValue() {
    assertAll(
        "output schema infer value checks",
        () -> assertFalse(createSourceConfig().getBoolean(OUTPUT_SCHEMA_INFER_VALUE_CONFIG)),
        () ->
            assertTrue(
                createSourceConfig(OUTPUT_SCHEMA_INFER_VALUE_CONFIG, "true")
                    .getBoolean(OUTPUT_SCHEMA_INFER_VALUE_CONFIG)),
        () -> assertInvalid(OUTPUT_SCHEMA_INFER_VALUE_CONFIG, "-1"));
  }

  @Test
  @DisplayName("test pipeline")
  void testPipeline() {
    assertAll(
        "fullDocument checks",
        () -> assertEquals(Optional.empty(), createSourceConfig().getPipeline()),
        () -> assertEquals(Optional.empty(), createSourceConfig(PIPELINE_CONFIG, "").getPipeline()),
        () ->
            assertEquals(Optional.empty(), createSourceConfig(PIPELINE_CONFIG, "[]").getPipeline()),
        () -> {
          String pipeline =
              "[{\"$match\": {\"operationType\": \"insert\"}}, {\"$addFields\": {\"Kafka\": \"Rules!\"}}]";
          List<Document> expectedPipeline =
              Document.parse(format("{p: %s}", pipeline)).getList("p", Document.class);
          assertEquals(
              Optional.of(expectedPipeline),
              createSourceConfig(PIPELINE_CONFIG, pipeline).getPipeline());
        },
        () ->
            assertEquals(
                Optional.empty(), createSourceConfig().getPipeline(COPY_EXISTING_PIPELINE_CONFIG)),
        () ->
            assertEquals(
                Optional.empty(),
                createSourceConfig(COPY_EXISTING_PIPELINE_CONFIG, "")
                    .getPipeline(COPY_EXISTING_PIPELINE_CONFIG)),
        () ->
            assertEquals(
                Optional.empty(),
                createSourceConfig(COPY_EXISTING_PIPELINE_CONFIG, "[]")
                    .getPipeline(COPY_EXISTING_PIPELINE_CONFIG)),
        () -> {
          String pipeline =
              "[{\"$match\": {\"operationType\": \"insert\"}}, {\"$addFields\": {\"Kafka\": \"Rules!\"}}]";
          List<Document> expectedPipeline =
              Document.parse(format("{p: %s}", pipeline)).getList("p", Document.class);
          assertEquals(
              Optional.of(expectedPipeline),
              createSourceConfig(COPY_EXISTING_PIPELINE_CONFIG, pipeline)
                  .getPipeline(COPY_EXISTING_PIPELINE_CONFIG));
        },
        () -> assertInvalid(PIPELINE_CONFIG, "not json"),
        () -> assertInvalid(PIPELINE_CONFIG, "{invalid: 'pipeline format'}"),
        () -> assertInvalid(COPY_EXISTING_PIPELINE_CONFIG, "not json"),
        () -> assertInvalid(COPY_EXISTING_PIPELINE_CONFIG, "{invalid: 'pipeline format'}"));
  }

  @Test
  @DisplayName("test copy existing namespace regex")
  void testCopyExistingNamespaceRegex() {
    assertAll(
        "copy existing namespace regex checks",
        () ->
            assertEquals("", createSourceConfig().getString(COPY_EXISTING_NAMESPACE_REGEX_CONFIG)),
        () ->
            assertEquals(
                ".*",
                createSourceConfig(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, ".*")
                    .getString(COPY_EXISTING_NAMESPACE_REGEX_CONFIG)),
        () -> assertInvalid(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "["));
  }

  @Test
  @DisplayName("test batchSize")
  void testBatchSize() {
    assertAll(
        "batchSize checks",
        () -> assertEquals(0, createSourceConfig().getInt(BATCH_SIZE_CONFIG)),
        () ->
            assertEquals(
                101, createSourceConfig(BATCH_SIZE_CONFIG, "101").getInt(BATCH_SIZE_CONFIG)),
        () -> assertInvalid(BATCH_SIZE_CONFIG, "-1"));
  }

  @Test
  @DisplayName("test collation")
  void testCollation() {
    assertAll(
        "collation checks",
        () -> assertEquals(Optional.empty(), createSourceConfig().getCollation()),
        () ->
            assertEquals(Optional.empty(), createSourceConfig(COLLATION_CONFIG, "").getCollation()),
        () ->
            assertEquals(
                Optional.of(Collation.builder().build()),
                createSourceConfig(COLLATION_CONFIG, "{}").getCollation()),
        () -> {
          Collation collation = Collation.builder().build();
          assertEquals(
              Optional.of(collation),
              createSourceConfig(COLLATION_CONFIG, collation.asDocument().toJson()).getCollation());
        },
        () -> {
          Collation collation =
              Collation.builder()
                  .locale("en")
                  .caseLevel(true)
                  .collationCaseFirst(CollationCaseFirst.OFF)
                  .collationStrength(CollationStrength.IDENTICAL)
                  .collationAlternate(CollationAlternate.SHIFTED)
                  .collationMaxVariable(CollationMaxVariable.SPACE)
                  .numericOrdering(true)
                  .normalization(true)
                  .backwards(true)
                  .build();
          assertEquals(
              Optional.of(collation),
              createSourceConfig(COLLATION_CONFIG, collation.asDocument().toJson()).getCollation());
        },
        () -> assertInvalid(COLLATION_CONFIG, "not a collation"));
  }

  @Test
  @DisplayName("test fullDocument")
  void testFullDocument() {
    assertAll(
        "fullDocument checks",
        () -> assertEquals(Optional.empty(), createSourceConfig().getFullDocument()),
        () ->
            assertEquals(
                Optional.empty(), createSourceConfig(FULL_DOCUMENT_CONFIG, "").getFullDocument()),
        () ->
            assertEquals(
                Optional.of(FullDocument.DEFAULT),
                createSourceConfig(FULL_DOCUMENT_CONFIG, FullDocument.DEFAULT.getValue())
                    .getFullDocument()),
        () ->
            assertEquals(
                Optional.of(FullDocument.UPDATE_LOOKUP),
                createSourceConfig(FULL_DOCUMENT_CONFIG, FullDocument.UPDATE_LOOKUP.getValue())
                    .getFullDocument()),
        () -> assertInvalid(FULL_DOCUMENT_CONFIG, "madeUp"));
  }

  @Test
  @DisplayName("test topic mapping")
  void testTopicMapping() {
    assertAll(
        "Topic mapping",
        () ->
            assertEquals(
                DefaultTopicMapper.class, createSourceConfig().getTopicMapper().getClass()),
        () ->
            assertEquals(
                TestTopicMapper.class,
                createSourceConfig(TOPIC_MAPPER_CONFIG, TestTopicMapper.class.getCanonicalName())
                    .getTopicMapper()
                    .getClass()),
        () ->
            assertThrows(ConfigException.class, () -> createSourceConfig(TOPIC_MAPPER_CONFIG, "")),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    createSourceConfig(TOPIC_MAPPER_CONFIG, "com.mongo.missing.TopicMapperClass")));
  }

  @Test
  @DisplayName("test topic prefix")
  void testTopicPrefix() {
    assertAll(
        "Topic prefix",
        () -> assertEquals("", createSourceConfig().getString(TOPIC_PREFIX_CONFIG)),
        () ->
            assertEquals(
                "prefix",
                createSourceConfig(TOPIC_PREFIX_CONFIG, "prefix").getString(TOPIC_PREFIX_CONFIG)));
  }

  @Test
  @DisplayName("test topic suffix")
  void testTopicSuffix() {
    assertAll(
        "Topic suffix",
        () -> assertEquals("", createSourceConfig().getString(TOPIC_SUFFIX_CONFIG)),
        () ->
            assertEquals(
                "suffix",
                createSourceConfig(TOPIC_SUFFIX_CONFIG, "suffix").getString(TOPIC_SUFFIX_CONFIG)));
  }

  @Test
  @DisplayName("test poll max batch size")
  void testPollMaxBatchSize() {
    assertAll(
        "Poll max batch size",
        () -> assertEquals(1000, createSourceConfig().getInt(POLL_MAX_BATCH_SIZE_CONFIG)),
        () ->
            assertEquals(
                100,
                createSourceConfig(POLL_MAX_BATCH_SIZE_CONFIG, "100")
                    .getInt(POLL_MAX_BATCH_SIZE_CONFIG)),
        () -> assertInvalid(POLL_MAX_BATCH_SIZE_CONFIG, "0"));
  }

  @Test
  @DisplayName("test poll await time ms")
  void testPollAwaitTimeMs() {
    assertAll(
        "Poll await time ms",
        () -> assertEquals(5000, createSourceConfig().getLong(POLL_AWAIT_TIME_MS_CONFIG)),
        () ->
            assertEquals(
                100,
                createSourceConfig(POLL_AWAIT_TIME_MS_CONFIG, "100")
                    .getLong(POLL_AWAIT_TIME_MS_CONFIG)),
        () -> assertInvalid(POLL_AWAIT_TIME_MS_CONFIG, "0"));
  }

  @Test
  @DisplayName("Test error configuration")
  void testErrorConfigurations() {
    assertAll(
        "Error configurations",
        () -> assertFalse(createSourceConfig().tolerateErrors()),
        () -> assertTrue(createSourceConfig(ERRORS_TOLERANCE_CONFIG, "all").tolerateErrors()),
        () -> assertTrue(createSourceConfig().logErrors()),
        () -> assertFalse(createSourceConfig(ERRORS_TOLERANCE_CONFIG, "all").logErrors()),
        () ->
            assertTrue(
                createSourceConfig()
                    .getString(ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG)
                    .isEmpty()),
        () -> assertInvalid(ERRORS_TOLERANCE_CONFIG, "Some"));
  }

  @Test
  @DisplayName("test heartbeat interval ms")
  void testHeartbeatIntervalMS() {
    assertAll(
        "heartbeat interval ms",
        () -> assertEquals(0, createSourceConfig().getLong(HEARTBEAT_INTERVAL_MS_CONFIG)),
        () ->
            assertEquals(
                100,
                createSourceConfig(HEARTBEAT_INTERVAL_MS_CONFIG, "100")
                    .getLong(HEARTBEAT_INTERVAL_MS_CONFIG)),
        () -> assertInvalid(HEARTBEAT_INTERVAL_MS_CONFIG, "-1"));
  }

  @Test
  @DisplayName("test heartbeat topic name")
  void testHeartbeatTopicName() {
    assertAll(
        "Heartbeat topic name",
        () ->
            assertEquals(
                "__mongodb_heartbeats",
                createSourceConfig().getString(HEARTBEAT_TOPIC_NAME_CONFIG)),
        () ->
            assertEquals(
                "__my_topic",
                createSourceConfig(HEARTBEAT_TOPIC_NAME_CONFIG, "__my_topic")
                    .getString(HEARTBEAT_TOPIC_NAME_CONFIG)));
  }

  private void assertInvalid(final String key, final String value) {
    assertInvalid(key, createConfigMap(key, value));
  }

  private void assertInvalid(final String invalidKey, final Map<String, String> configMap) {
    assertFalse(
        MongoSourceConfig.CONFIG.validateAll(configMap).get(invalidKey).errorMessages().isEmpty());
    assertThrows(ConfigException.class, () -> new MongoSourceConfig(configMap));
  }
}
