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
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_ALLOW_DISK_USE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_ALLOW_DISK_USE_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_MAX_THREADS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_NAMESPACE_REGEX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COPY_EXISTING_QUEUE_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_LOG_ENABLE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.ERRORS_TOLERANCE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.FULL_DOCUMENT_BEFORE_CHANGE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.FULL_DOCUMENT_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.HEARTBEAT_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_KEY_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_FORMAT_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OUTPUT_SCHEMA_INFER_VALUE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OVERRIDE_ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OVERRIDE_ERRORS_LOG_ENABLE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.OVERRIDE_ERRORS_TOLERANCE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_AWAIT_TIME_MS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.POLL_MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.SPLIT_LARGE_EVENT_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_CONFIG_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_MAX_THREADS_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_COPY_EXISTING_QUEUE_SIZE_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_DEFAULT;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_PREFIX_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.TOPIC_SUFFIX_CONFIG;
import static com.mongodb.kafka.connect.source.SourceTestHelper.CLIENT_URI_AUTH_SETTINGS;
import static com.mongodb.kafka.connect.source.SourceTestHelper.CLIENT_URI_DEFAULT_SETTINGS;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import org.bson.BsonTimestamp;
import org.bson.Document;

import com.mongodb.client.model.Collation;
import com.mongodb.client.model.CollationAlternate;
import com.mongodb.client.model.CollationCaseFirst;
import com.mongodb.client.model.CollationMaxVariable;
import com.mongodb.client.model.CollationStrength;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.FullDocumentBeforeChange;

import com.mongodb.kafka.connect.source.MongoSourceConfig.OutputFormat;
import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.CopyExistingConfig;
import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.StartupMode;
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
                Optional.empty(),
                createSourceConfig(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue())
                    .getStartupConfig()
                    .copyExistingConfig()
                    .pipeline()),
        () -> {
          Map<String, String> props = new HashMap<>();
          props.put(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
          props.put(STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG, "");
          assertEquals(
              Optional.empty(),
              createSourceConfig(props).getStartupConfig().copyExistingConfig().pipeline());
        },
        () -> {
          Map<String, String> props = new HashMap<>();
          props.put(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
          props.put(STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG, "[]");
          assertEquals(
              Optional.empty(),
              createSourceConfig(props).getStartupConfig().copyExistingConfig().pipeline());
        },
        () -> {
          String pipeline =
              "[{\"$match\": {\"operationType\": \"insert\"}}, {\"$addFields\": {\"Kafka\": \"Rules!\"}}]";
          List<Document> expectedPipeline =
              Document.parse(format("{p: %s}", pipeline)).getList("p", Document.class);
          Map<String, String> props = new HashMap<>();
          props.put(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
          props.put(STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG, pipeline);
          assertEquals(
              Optional.of(expectedPipeline),
              createSourceConfig(props).getStartupConfig().copyExistingConfig().pipeline());
        },
        () -> assertInvalid(PIPELINE_CONFIG, "not json"),
        () -> assertInvalid(PIPELINE_CONFIG, "{invalid: 'pipeline format'}"),
        () -> assertInvalid(STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG, "not json"),
        () ->
            assertInvalid(
                STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG, "{invalid: 'pipeline format'}"));
  }

  @Test
  @DisplayName("test copy existing namespace regex")
  void testCopyExistingNamespaceRegex() {
    assertAll(
        "copy existing namespace regex checks",
        () ->
            assertEquals(
                "",
                createSourceConfig(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue())
                    .getStartupConfig()
                    .copyExistingConfig()
                    .namespaceRegex()),
        () -> {
          Map<String, String> props = new HashMap<>();
          props.put(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());
          props.put(STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG, ".*");
          assertEquals(
              ".*",
              createSourceConfig(props).getStartupConfig().copyExistingConfig().namespaceRegex());
        },
        () -> assertInvalid(STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "["));
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
  @DisplayName("test split large event")
  void testSplitLargeEvent() {
    assertAll(
        "split large event checks",
        () -> assertFalse(createSourceConfig().getSplitLargeEvent()),
        () ->
            assertFalse(createSourceConfig(SPLIT_LARGE_EVENT_CONFIG, "false").getSplitLargeEvent()),
        () -> assertTrue(createSourceConfig(SPLIT_LARGE_EVENT_CONFIG, "true").getSplitLargeEvent()),
        () -> assertInvalid(SPLIT_LARGE_EVENT_CONFIG, "invalid"));
  }

  @Test
  @DisplayName("test fullDocumentBeforeChange")
  void testFullDocumentBeforeChange() {
    assertAll(
        "fullDocumentBeforeChange checks",
        () -> assertFalse(createSourceConfig().getFullDocumentBeforeChange().isPresent()),
        () ->
            assertFalse(
                createSourceConfig(FULL_DOCUMENT_BEFORE_CHANGE_CONFIG, "")
                    .getFullDocumentBeforeChange()
                    .isPresent()),
        () ->
            assertEquals(
                Optional.of(FullDocumentBeforeChange.DEFAULT),
                createSourceConfig(
                        FULL_DOCUMENT_BEFORE_CHANGE_CONFIG,
                        FullDocumentBeforeChange.DEFAULT.getValue())
                    .getFullDocumentBeforeChange()),
        () ->
            assertEquals(
                Optional.of(FullDocumentBeforeChange.WHEN_AVAILABLE),
                createSourceConfig(
                        FULL_DOCUMENT_BEFORE_CHANGE_CONFIG,
                        FullDocumentBeforeChange.WHEN_AVAILABLE.getValue())
                    .getFullDocumentBeforeChange()),
        () -> assertInvalid(FULL_DOCUMENT_BEFORE_CHANGE_CONFIG, "madeUp"));
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
  @DisplayName("Test error configuration overrides")
  void testErrorConfigurationOverrides() {
    assertAll(
        "Error configuration overrides",
        () ->
            assertTrue(
                createSourceConfig(OVERRIDE_ERRORS_TOLERANCE_CONFIG, "all").tolerateErrors()),
        () ->
            assertFalse(
                createSourceConfig(OVERRIDE_ERRORS_TOLERANCE_CONFIG, "none").tolerateErrors()),
        () ->
            assertFalse(
                createSourceConfig(
                        format(
                            "{'%s': '%s', '%s': '%s'}",
                            ERRORS_TOLERANCE_CONFIG,
                            "all",
                            OVERRIDE_ERRORS_TOLERANCE_CONFIG,
                            "none"))
                    .tolerateErrors()),
        () ->
            assertTrue(
                createSourceConfig(
                        format(
                            "{'%s': '%s', '%s': '%s'}",
                            ERRORS_TOLERANCE_CONFIG,
                            "none",
                            OVERRIDE_ERRORS_TOLERANCE_CONFIG,
                            "all"))
                    .tolerateErrors()),
        () -> assertTrue(createSourceConfig(OVERRIDE_ERRORS_LOG_ENABLE_CONFIG, "true").logErrors()),
        () ->
            assertTrue(
                createSourceConfig(
                        format(
                            "{'%s': '%s', '%s': '%s'}",
                            ERRORS_LOG_ENABLE_CONFIG,
                            "false",
                            OVERRIDE_ERRORS_LOG_ENABLE_CONFIG,
                            "true"))
                    .logErrors()),
        () -> assertEquals("", createSourceConfig().getDlqTopic()),
        () ->
            assertEquals(
                "dlq",
                createSourceConfig(OVERRIDE_ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "dlq")
                    .getDlqTopic()),
        () ->
            assertEquals(
                "dlq",
                createSourceConfig(
                        format(
                            "{'%s': '%s', '%s': '%s'}",
                            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
                            "qld",
                            OVERRIDE_ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
                            "dlq"))
                    .getDlqTopic()),
        () ->
            assertEquals(
                "",
                createSourceConfig(
                        format(
                            "{'%s': '%s', '%s': '%s'}",
                            ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
                            "qld",
                            OVERRIDE_ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
                            ""))
                    .getDlqTopic()));
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

  @Nested
  final class StartupModeTest {
    @Test
    void startupMode() {
      assertAll(
          () ->
              assertThrows(
                  AssertionError.class,
                  () -> createSourceConfig().getStartupConfig().timestampConfig()),
          () ->
              assertThrows(
                  AssertionError.class,
                  () -> createSourceConfig().getStartupConfig().copyExistingConfig()),
          () ->
              assertThrows(
                  AssertionError.class,
                  () ->
                      createSourceConfig(STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue())
                          .getStartupConfig()
                          .copyExistingConfig()),
          () ->
              assertThrows(
                  AssertionError.class,
                  () ->
                      createSourceConfig(
                              STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue())
                          .getStartupConfig()
                          .timestampConfig()),
          () ->
              assertSame(StartupMode.LATEST, createSourceConfig().getStartupConfig().startupMode()),
          () ->
              assertSame(
                  StartupMode.LATEST,
                  createSourceConfig(STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG, "0")
                      .getStartupConfig()
                      .startupMode()),
          () ->
              assertSame(
                  StartupMode.TIMESTAMP,
                  createSourceConfig(STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue())
                      .getStartupConfig()
                      .startupMode()),
          () ->
              assertSame(
                  StartupMode.LATEST,
                  createSourceConfig(
                          STARTUP_MODE_CONFIG, STARTUP_MODE_CONFIG_DEFAULT.propertyValue())
                      .getStartupConfig()
                      .startupMode()),
          () ->
              assertSame(
                  StartupMode.COPY_EXISTING,
                  createSourceConfig(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue())
                      .getStartupConfig()
                      .startupMode()),
          () ->
              assertSame(
                  StartupMode.LATEST,
                  createSourceConfig(COPY_EXISTING_CONFIG, String.valueOf(COPY_EXISTING_DEFAULT))
                      .getStartupConfig()
                      .startupMode()),
          () ->
              assertSame(
                  StartupMode.LATEST,
                  createSourceConfig(STARTUP_MODE_COPY_EXISTING_MAX_THREADS_CONFIG, "1")
                      .getStartupConfig()
                      .startupMode()),
          () ->
              assertSame(
                  StartupMode.COPY_EXISTING,
                  createSourceConfig(COPY_EXISTING_CONFIG, Boolean.TRUE.toString())
                      .getStartupConfig()
                      .startupMode()),
          () -> {
            Map<String, String> props = new HashMap<>();
            props.put(COPY_EXISTING_CONFIG, Boolean.TRUE.toString());
            props.put(STARTUP_MODE_CONFIG, StartupMode.LATEST.propertyValue());
            assertSame(
                StartupMode.LATEST, createSourceConfig(props).getStartupConfig().startupMode());
          },
          () -> {
            Map<String, String> props = new HashMap<>();
            props.put(COPY_EXISTING_CONFIG, Boolean.TRUE.toString());
            props.put(STARTUP_MODE_CONFIG, STARTUP_MODE_CONFIG_DEFAULT.propertyValue());
            assertSame(
                StartupMode.COPY_EXISTING,
                createSourceConfig(props).getStartupConfig().startupMode());
          },
          () -> assertInvalid(STARTUP_MODE_CONFIG, "invalid"));
    }

    @Test
    void timestampStartAtOperationTime() {
      assertAll(
          () ->
              assertFalse(
                  createSourceConfig(STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue())
                      .getStartupConfig()
                      .timestampConfig()
                      .startAtOperationTime()
                      .isPresent()),
          () -> {
            Map<String, String> props = new HashMap<>();
            props.put(STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue());
            props.put(
                STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG,
                STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_DEFAULT);
            assertFalse(
                createSourceConfig(props)
                    .getStartupConfig()
                    .timestampConfig()
                    .startAtOperationTime()
                    .isPresent());
          },
          () -> {
            Map<String, String> props = new HashMap<>();
            props.put(STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue());
            props.put(
                STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG,
                "{\"$timestamp\": {\"t\": 30, \"i\": 0}}");
            assertEquals(
                new BsonTimestamp(30, 0),
                createSourceConfig(props)
                    .getStartupConfig()
                    .timestampConfig()
                    .startAtOperationTime()
                    .get());
          },
          () -> {
            Map<String, String> props = new HashMap<>();
            props.put(STARTUP_MODE_CONFIG, StartupMode.TIMESTAMP.propertyValue());
            props.put(STARTUP_MODE_TIMESTAMP_START_AT_OPERATION_TIME_CONFIG, "abc");
            assertThrows(ConfigException.class, () -> createSourceConfig(props));
          });
    }

    @Test
    void copyExistingProperties() {
      Document pipelineStage =
          new Document("$match", new Document("myInt", new Document("$gt", 10)));
      assertAll(
          () -> {
            Map<String, String> props = new HashMap<>();
            props.put(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());

            props.put(STARTUP_MODE_COPY_EXISTING_MAX_THREADS_CONFIG, "123");
            props.put(COPY_EXISTING_MAX_THREADS_CONFIG, "321");

            props.put(COPY_EXISTING_QUEUE_SIZE_CONFIG, "456");

            props.put(
                STARTUP_MODE_COPY_EXISTING_PIPELINE_CONFIG, "[" + pipelineStage.toJson() + "]");
            props.put(COPY_EXISTING_PIPELINE_CONFIG, "[]");

            props.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "abc");

            props.put(
                STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_CONFIG,
                String.valueOf(!STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_DEFAULT));
            props.put(
                COPY_EXISTING_ALLOW_DISK_USE_CONFIG,
                String.valueOf(COPY_EXISTING_ALLOW_DISK_USE_DEFAULT));

            CopyExistingConfig copyExistingConfig =
                createSourceConfig(props).getStartupConfig().copyExistingConfig();

            assertEquals(123, copyExistingConfig.maxThreads());
            assertEquals(456, copyExistingConfig.queueSize());
            assertEquals(singletonList(pipelineStage), copyExistingConfig.pipeline().get());
            assertEquals("abc", copyExistingConfig.namespaceRegex());
            assertEquals(
                !STARTUP_MODE_COPY_EXISTING_ALLOW_DISK_USE_DEFAULT,
                copyExistingConfig.allowDiskUse());
          },
          () -> {
            Map<String, String> props = new HashMap<>();
            props.put(STARTUP_MODE_CONFIG, StartupMode.COPY_EXISTING.propertyValue());

            props.put(STARTUP_MODE_COPY_EXISTING_MAX_THREADS_CONFIG, "123");

            props.put(STARTUP_MODE_COPY_EXISTING_QUEUE_SIZE_CONFIG, "456");
            props.put(COPY_EXISTING_QUEUE_SIZE_CONFIG, "654");

            props.put(COPY_EXISTING_PIPELINE_CONFIG, "[" + pipelineStage.toJson() + "]");

            props.put(STARTUP_MODE_COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "abc");
            props.put(COPY_EXISTING_NAMESPACE_REGEX_CONFIG, "cba");

            props.put(
                COPY_EXISTING_ALLOW_DISK_USE_CONFIG,
                String.valueOf(!COPY_EXISTING_ALLOW_DISK_USE_DEFAULT));

            CopyExistingConfig copyExistingConfig =
                createSourceConfig(props).getStartupConfig().copyExistingConfig();

            assertEquals(123, copyExistingConfig.maxThreads());
            assertEquals(456, copyExistingConfig.queueSize());
            assertEquals(singletonList(pipelineStage), copyExistingConfig.pipeline().get());
            assertEquals("abc", copyExistingConfig.namespaceRegex());
            assertEquals(!COPY_EXISTING_ALLOW_DISK_USE_DEFAULT, copyExistingConfig.allowDiskUse());
          });
    }
  }

  private static void assertInvalid(final String key, final String value) {
    assertInvalid(key, createConfigMap(key, value));
  }

  private static void assertInvalid(final String invalidKey, final Map<String, String> configMap) {
    assertFalse(
        MongoSourceConfig.CONFIG.validateAll(configMap).get(invalidKey).errorMessages().isEmpty());
    assertThrows(ConfigException.class, () -> new MongoSourceConfig(configMap));
  }
}
