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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_REGEX_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.createOverrideKey;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.CHANGE_DATA_CAPTURE_HANDLER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DATABASE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_MAPPING_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_REGEXP_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.POST_PROCESSOR_CHAIN_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.WRITEMODEL_STRATEGY_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.CLIENT_URI_AUTH_SETTINGS;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.CLIENT_URI_DEFAULT_SETTINGS;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createConfigMap;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createSinkConfig;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.connect.runtime.SinkConnectorConfig.TOPICS_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.mongodb.kafka.connect.sink.cdc.debezium.mongodb.MongoDbHandler;
import com.mongodb.kafka.connect.sink.cdc.debezium.rdbms.RdbmsHandler;
import com.mongodb.kafka.connect.sink.cdc.debezium.rdbms.mysql.MysqlHandler;
import com.mongodb.kafka.connect.sink.cdc.debezium.rdbms.postgres.PostgresHandler;
import com.mongodb.kafka.connect.sink.namespace.mapping.DefaultNamespaceMapper;
import com.mongodb.kafka.connect.sink.processor.BlacklistValueProjector;
import com.mongodb.kafka.connect.sink.processor.DocumentIdAdder;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;
import com.mongodb.kafka.connect.sink.processor.WhitelistKeyProjector;
import com.mongodb.kafka.connect.sink.processor.field.renaming.RenameByMapping;
import com.mongodb.kafka.connect.sink.processor.field.renaming.RenameByRegExp;
import com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.KafkaMetaDataStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialKeyStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInValueStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.UuidStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.DeleteOneDefaultStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneBusinessKeyStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.UpdateOneBusinessKeyTimestampStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.UpdateOneTimestampsStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;

import com.github.jcustenborder.kafka.connect.utils.config.MarkdownFormatter;

@RunWith(JUnitPlatform.class)
class MongoSinkConfigTest {
  private static final Pattern EMPTY_PATTERN = Pattern.compile("");

  @Test
  @DisplayName("build config doc (no test)")
  // CHECKSTYLE:OFF
  void doc() {
    System.out.println(MongoSinkConfig.CONFIG.toRst());
    System.out.println(MarkdownFormatter.toMarkdown(MongoSinkConfig.CONFIG));
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
                CLIENT_URI_DEFAULT_SETTINGS, createSinkConfig().getConnectionString().toString()),
        () ->
            assertEquals(
                CLIENT_URI_AUTH_SETTINGS,
                createSinkConfig(CONNECTION_URI_CONFIG, CLIENT_URI_AUTH_SETTINGS)
                    .getConnectionString()
                    .toString()),
        () -> assertInvalid(CONNECTION_URI_CONFIG, "invalid connection string"));
  }

  @Test
  @DisplayName("test topics")
  void testTopics() {
    assertAll(
        "topics",
        () ->
            assertEquals(
                singletonList("a"),
                createSinkConfig(TOPICS_CONFIG, "a").getTopics().orElse(emptyList())),
        () ->
            assertEquals(
                asList("a", "b", "c"),
                createSinkConfig(TOPICS_CONFIG, "a,b,c").getTopics().orElse(emptyList())),
        () -> assertInvalid(TOPICS_CONFIG, ""));
  }

  @Test
  @DisplayName("test validation")
  void testValidation() {
    Map<String, String> configMap =
        createConfigMap(
            format(
                "{'%s': 'topic1, topic2', '%s': 'otherDB', '%s': 'coll2'}",
                TOPICS_CONFIG,
                createOverrideKey("topic2", DATABASE_CONFIG),
                createOverrideKey("topic2", COLLECTION_CONFIG)));

    Map<String, ConfigValue> validateAllMap = MongoSinkConfig.CONFIG.validateAll(configMap);

    validateAllMap.values().forEach(v -> assertTrue(v.errorMessages().isEmpty()));
    Set<String> configNames =
        validateAllMap.values().stream().map(ConfigValue::name).collect(Collectors.toSet());

    Set<String> expectedKeys = new HashSet<>(MongoSinkConfig.CONFIG.configKeys().keySet());
    expectedKeys.addAll(MongoSinkTopicConfig.CONFIG.configKeys().keySet());
    // Remove ignored configs
    expectedKeys.removeAll(MongoSinkConfig.IGNORED_CONFIGS);
    expectedKeys.removeAll(MongoSinkTopicConfig.IGNORED_CONFIGS);

    // Added declared overrides
    expectedKeys.addAll(configMap.keySet());

    assertEquals(expectedKeys, configNames);
  }

  @Test
  @DisplayName("test topic regex")
  void testTopicRegex() {
    assertAll(
        "topicsRegex",
        () ->
            assertPattern(
                ".*",
                createSinkConfig(TOPICS_REGEX_CONFIG, ".*").getTopicRegex().orElse(EMPTY_PATTERN)),
        () -> assertInvalid(TOPICS_REGEX_CONFIG, "["),
        () ->
            assertInvalid(
                DATABASE_CONFIG,
                createConfigMap(
                    format("{'%s': '.*', '%s': ''}", TOPICS_REGEX_CONFIG, DATABASE_CONFIG))),
        () ->
            assertDoesNotThrow(
                () ->
                    createSinkConfig(TOPICS_REGEX_CONFIG, "topic-(.*)")
                        .getMongoSinkTopicConfig("topic-1")),
        () ->
            assertThrows(
                ConfigException.class,
                () ->
                    createSinkConfig(TOPICS_REGEX_CONFIG, "topic-(.*)")
                        .getMongoSinkTopicConfig("alpha")));
  }

  @Test
  @DisplayName("test topics and topic regex")
  void testTopicsAndTopicRegex() {
    Map<String, String> configMap = createConfigMap();
    configMap.put(TOPICS_REGEX_CONFIG, ".*");
    assertInvalid(TOPICS_CONFIG, configMap);
  }

  @Test
  @DisplayName("test missing database name")
  void testMissingDatabaseName() {
    assertInvalid(DATABASE_CONFIG, "");
  }

  @Test
  @DisplayName("test topic overrides")
  void testTopicOverrides() {
    MongoSinkConfig cfg =
        createSinkConfig(
            format(
                "{'%s': 'topic,t2', '%s': 'otherDB', '%s': 'coll2'}",
                TOPICS_CONFIG,
                createOverrideKey("t2", DATABASE_CONFIG),
                createOverrideKey("t2", COLLECTION_CONFIG)));
    assertThat(cfg.getTopics().orElse(emptyList()), containsInAnyOrder("topic", "t2"));

    assertEquals("myDB", cfg.getMongoSinkTopicConfig("topic").getString(DATABASE_CONFIG));
    assertEquals("", cfg.getMongoSinkTopicConfig("topic").getString(COLLECTION_CONFIG));
    assertEquals("otherDB", cfg.getMongoSinkTopicConfig("t2").getString(DATABASE_CONFIG));
    assertEquals("coll2", cfg.getMongoSinkTopicConfig("t2").getString(COLLECTION_CONFIG));
  }

  @Test
  @DisplayName("test topic regex with overrides")
  void testTopicRegexWithOverrides() {
    MongoSinkConfig cfg =
        createSinkConfig(
            format(
                "{'%s': 't(.*)', '%s': 'otherDB', '%s': 'coll2', '%s': 'coll3'}",
                TOPICS_REGEX_CONFIG,
                createOverrideKey("t2", DATABASE_CONFIG),
                createOverrideKey("t2", COLLECTION_CONFIG),
                createOverrideKey("noMatch", COLLECTION_CONFIG)));
    assertPattern("t(.*)", cfg.getTopicRegex().orElse(EMPTY_PATTERN));
    assertEquals("myDB", cfg.getMongoSinkTopicConfig("topic").getString(DATABASE_CONFIG));
    assertEquals("", cfg.getMongoSinkTopicConfig("topic").getString(COLLECTION_CONFIG));
    assertEquals("otherDB", cfg.getMongoSinkTopicConfig("t2").getString(DATABASE_CONFIG));
    assertEquals("coll2", cfg.getMongoSinkTopicConfig("t2").getString(COLLECTION_CONFIG));
    assertThrows(ConfigException.class, () -> cfg.getMongoSinkTopicConfig("noMatch"));
    assertThrows(
        ConfigException.class,
        () ->
            createSinkConfig(
                format(
                    "{'%s': 't(.*)', '%s': 'made up'}",
                    TOPICS_REGEX_CONFIG, createOverrideKey("t2", KEY_PROJECTION_TYPE_CONFIG))));
  }

  @Test
  @DisplayName("test K/V projection with invalid projection type")
  void testProjectionWithInvalidProjectionTypes() {
    assertAll(
        "with invalid projection type",
        () -> assertInvalid(KEY_PROJECTION_TYPE_CONFIG, "made up"),
        () -> assertInvalid(VALUE_PROJECTION_TYPE_CONFIG, "made up"));
  }

  @Test
  @DisplayName("test correct field set for K/V projection when type is 'blacklist'")
  void testCorrectFieldSetForKeyAndValueBlacklistProjectionList() {
    String fieldList = " ,field1, field2.subA ,  field2.subB,  field3.** , ,,  ";
    Set<String> blacklisted =
        new HashSet<>(asList("field1", "field2.subA", "field2.subB", "field3.**"));
    MongoSinkConfig keyConfig =
        createSinkConfig(
            format(
                "{'%s': '%s', '%s': 'blacklist', '%s': '%s'}",
                DOCUMENT_ID_STRATEGY_CONFIG,
                PartialKeyStrategy.class.getName(),
                KEY_PROJECTION_TYPE_CONFIG,
                KEY_PROJECTION_LIST_CONFIG,
                fieldList));

    IdStrategy idStrategy = keyConfig.getMongoSinkTopicConfig(TEST_TOPIC).getIdStrategy();
    assertTrue(idStrategy instanceof PartialKeyStrategy);
    assertIterableEquals(
        ((PartialKeyStrategy) idStrategy).getFieldProjector().getFields(), blacklisted);

    MongoSinkConfig valueConfig =
        createSinkConfig(
            format(
                "{'%s': '%s', '%s': 'blacklist', '%s': '%s'}",
                DOCUMENT_ID_STRATEGY_CONFIG,
                PartialValueStrategy.class.getName(),
                VALUE_PROJECTION_TYPE_CONFIG,
                VALUE_PROJECTION_LIST_CONFIG,
                fieldList));

    idStrategy = valueConfig.getMongoSinkTopicConfig(TEST_TOPIC).getIdStrategy();
    assertTrue(idStrategy instanceof PartialValueStrategy);
    assertIterableEquals(
        ((PartialValueStrategy) idStrategy).getFieldProjector().getFields(), blacklisted);
  }

  @Test
  @DisplayName("test correct field set for K/V projection when type is 'whitelist'")
  void testCorrectFieldSetForKeyAndValueWhiteListProjectionList() {
    String fieldList =
        " ,field1.**, field2.*.subSubA ,  field2.subB.*,  field3.subC.subSubD , ,,  ";

    Set<String> whitelisted =
        new HashSet<>(
            asList(
                "field1",
                "field1.**",
                "field2",
                "field2.*",
                "field2.*.subSubA",
                "field2.subB",
                "field2.subB.*",
                "field3",
                "field3.subC",
                "field3.subC.subSubD"));

    MongoSinkConfig keyConfig =
        createSinkConfig(
            format(
                "{'%s': '%s', '%s': 'whitelist', '%s': '%s'}",
                DOCUMENT_ID_STRATEGY_CONFIG,
                PartialKeyStrategy.class.getName(),
                KEY_PROJECTION_TYPE_CONFIG,
                KEY_PROJECTION_LIST_CONFIG,
                fieldList));

    IdStrategy idStrategy = keyConfig.getMongoSinkTopicConfig(TEST_TOPIC).getIdStrategy();
    assertTrue(idStrategy instanceof PartialKeyStrategy);
    assertIterableEquals(
        ((PartialKeyStrategy) idStrategy).getFieldProjector().getFields(), whitelisted);

    MongoSinkConfig valueConfig =
        createSinkConfig(
            format(
                "{'%s': '%s', '%s': 'whitelist', '%s': '%s'}",
                DOCUMENT_ID_STRATEGY_CONFIG,
                PartialValueStrategy.class.getName(),
                VALUE_PROJECTION_TYPE_CONFIG,
                VALUE_PROJECTION_LIST_CONFIG,
                fieldList));

    idStrategy = valueConfig.getMongoSinkTopicConfig(TEST_TOPIC).getIdStrategy();
    assertTrue(idStrategy instanceof PartialValueStrategy);
    assertIterableEquals(
        ((PartialValueStrategy) idStrategy).getFieldProjector().getFields(), whitelisted);
  }

  @TestFactory
  @DisplayName("test for invalid idStrategies")
  Collection<DynamicTest> testForInvalidIdStrategies() {
    List<DynamicTest> idStrategyTests = new ArrayList<>();
    String json = "{'%s': '%s', '%s': '%s'}";
    List<String> tests =
        asList(
            BsonOidStrategy.class.getName(),
            FullKeyStrategy.class.getName(),
            KafkaMetaDataStrategy.class.getName(),
            PartialKeyStrategy.class.getName(),
            PartialValueStrategy.class.getName(),
            ProvidedInKeyStrategy.class.getName(),
            ProvidedInValueStrategy.class.getName(),
            UuidStrategy.class.getName());

    tests.forEach(
        s -> {
          String projectionType =
              s.contains("Value") ? VALUE_PROJECTION_TYPE_CONFIG : KEY_PROJECTION_TYPE_CONFIG;
          idStrategyTests.add(
              dynamicTest(
                  "blacklist: test id strategy for " + s,
                  () -> {
                    MongoSinkConfig cfg =
                        createSinkConfig(
                            format(
                                json, projectionType, "blacklist", DOCUMENT_ID_STRATEGY_CONFIG, s));
                    assertEquals(
                        cfg.getMongoSinkTopicConfig(TEST_TOPIC)
                            .getIdStrategy()
                            .getClass()
                            .getName(),
                        s);
                  }));

          idStrategyTests.add(
              dynamicTest(
                  "whiltelist: test id strategy for " + s,
                  () -> {
                    MongoSinkConfig cfg =
                        createSinkConfig(
                            format(
                                json, projectionType, "whitelist", DOCUMENT_ID_STRATEGY_CONFIG, s));
                    assertEquals(
                        cfg.getMongoSinkTopicConfig(TEST_TOPIC)
                            .getIdStrategy()
                            .getClass()
                            .getName(),
                        s);
                  }));
        });
    return idStrategyTests;
  }

  @Test
  @DisplayName("test invalid id strategy names")
  void testInvalidIdStrategyNames() {
    assertAll(
        "with invalid id strategy names",
        () -> assertInvalid(DOCUMENT_ID_STRATEGY_CONFIG, ""),
        () -> assertInvalid(DOCUMENT_ID_STRATEGY_CONFIG, "not a class format"),
        () -> assertInvalid(DOCUMENT_ID_STRATEGY_CONFIG, "com.example.kafka.test.Strategy"));
  }

  @Test
  @DisplayName("test invalid post processor chains")
  void testInvalidPostProcessorChainNames() {
    assertAll(
        "with invalid post processor chains",
        () -> {
          Exception e = assertInvalid(POST_PROCESSOR_CHAIN_CONFIG, "not a class format");
          assertEquals(
              "Invalid value [not a class format] "
                  + "for configuration post.processor.chain: "
                  + "Does not match expected class pattern.",
              e.getMessage());
        },
        () -> {
          Exception e =
              assertInvalid(
                  POST_PROCESSOR_CHAIN_CONFIG,
                  "com.example.kafka.test.Strategy,com.example.alpha.Bravo");
          assertEquals(
              "Invalid value [com.example.kafka.test.Strategy, com.example.alpha.Bravo] "
                  + "for configuration post.processor.chain: "
                  + "Class not found: com.example.kafka.test.Strategy",
              e.getMessage());
        });
  }

  @TestFactory
  @DisplayName("test valid change data capture handler names")
  Collection<DynamicTest> testValidChangeDataCaptureHandlerNames() {
    List<DynamicTest> tests = new ArrayList<>();
    String json = "{'%s': '%s'}";
    List<String> cdcHandlers =
        asList(
            MongoDbHandler.class.getName(),
            RdbmsHandler.class.getName(),
            MysqlHandler.class.getName(),
            PostgresHandler.class.getName());
    cdcHandlers.forEach(
        s ->
            tests.add(
                dynamicTest(
                    "cdc Handler for " + s,
                    () -> {
                      MongoSinkConfig cfg =
                          createSinkConfig(format(json, CHANGE_DATA_CAPTURE_HANDLER_CONFIG, s));
                      assertEquals(
                          cfg.getMongoSinkTopicConfig(TEST_TOPIC)
                              .getCdcHandler()
                              .get()
                              .getClass()
                              .getName(),
                          s);
                    })));
    return tests;
  }

  @Test
  @DisplayName("test invalid change data capture handler names")
  void testInvalidChangeDataCaptureHandlerNames() {
    assertAll(
        "with invalid projection type",
        () -> assertInvalid(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, "not a class format"),
        () ->
            assertInvalid(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, "com.example.kafka.test.CDCHandler"));
  }

  @Test
  @DisplayName("test parse json rename field name mappings")
  void testParseJsonRenameFieldnameMappings() {
    String postProcessors = RenameByMapping.class.getName();
    String json = format("{'%s': '%s', '%%s': '%%s'}", POST_PROCESSOR_CHAIN_CONFIG, postProcessors);

    assertAll(
        "field name mappings",
        () -> {
          MongoSinkConfig cfg = createSinkConfig(format(json, FIELD_RENAMER_MAPPING_CONFIG, "[]"));
          List<PostProcessor> pp =
              cfg.getMongoSinkTopicConfig(TEST_TOPIC).getPostProcessors().getPostProcessorList();

          assertEquals(2, pp.size());
          assertTrue(pp.get(1) instanceof RenameByMapping);
        },
        () -> {
          MongoSinkConfig cfg =
              createSinkConfig(
                  format(
                      json,
                      FIELD_RENAMER_MAPPING_CONFIG,
                      "[{\"oldName\":\"key.fieldA\",\"newName\":\"field1\"},{\"oldName\":\"value.xyz\",\"newName\":\"abc\"}]"));
          List<PostProcessor> pp =
              cfg.getMongoSinkTopicConfig(TEST_TOPIC).getPostProcessors().getPostProcessorList();

          assertEquals(2, pp.size());
          assertTrue(pp.get(1) instanceof RenameByMapping);
        },
        () ->
            assertInvalid(
                FIELD_RENAMER_MAPPING_CONFIG,
                createConfigMap(format(json, FIELD_RENAMER_MAPPING_CONFIG, "]not: json}"))));
  }

  @Test
  @DisplayName("test parse json rename regexp settings")
  void testParseJsonRenameRegExpSettings() {
    String postProcessors = RenameByRegExp.class.getName();
    String json = format("{'%s': '%s', '%%s': '%%s'}", POST_PROCESSOR_CHAIN_CONFIG, postProcessors);
    assertAll(
        "field name mappings",
        () -> {
          MongoSinkConfig cfg = createSinkConfig(format(json, FIELD_RENAMER_REGEXP_CONFIG, "[]"));
          List<PostProcessor> pp =
              cfg.getMongoSinkTopicConfig(TEST_TOPIC).getPostProcessors().getPostProcessorList();

          assertEquals(2, pp.size());
          assertTrue(pp.get(1) instanceof RenameByRegExp);
        },
        () -> {
          MongoSinkConfig cfg =
              createSinkConfig(
                  format(
                      json,
                      FIELD_RENAMER_REGEXP_CONFIG,
                      "[{\"regexp\":\"^key\\\\\\\\..*my.*$\",\"pattern\":\"my\",\"replace\":\"\"},"
                          + "{\"regexp\":\"^value\\\\\\\\..*$\",\"pattern\":\"\\\\\\\\.\",\"replace\":\"_\"}]"));
          List<PostProcessor> pp =
              cfg.getMongoSinkTopicConfig(TEST_TOPIC).getPostProcessors().getPostProcessorList();
          assertEquals(2, pp.size());
          assertTrue(pp.get(1) instanceof RenameByRegExp);
        },
        () -> {
          MongoSinkConfig cfg =
              createSinkConfig(
                  format(
                      json,
                      FIELD_RENAMER_REGEXP_CONFIG,
                      "[{\"regexp\":\"^key\\\\..*my.*$\",\"pattern\":\"my\",\"replace\":\"\"},"
                          + "{\"regexp\":\"^value\\\\..*$\",\"pattern\":\"\\\\.\",\"replace\":\"_\"}]"));
          List<PostProcessor> pp =
              cfg.getMongoSinkTopicConfig(TEST_TOPIC).getPostProcessors().getPostProcessorList();
          assertEquals(2, pp.size());
          assertTrue(pp.get(1) instanceof RenameByRegExp);
        },
        () ->
            assertInvalid(
                FIELD_RENAMER_REGEXP_CONFIG,
                createConfigMap(format(json, FIELD_RENAMER_REGEXP_CONFIG, "]not: json}"))));
  }

  @Test
  @DisplayName("test namespace mapper")
  void testNamespaceMapper() {
    assertAll(
        "with invalid projection type",
        () ->
            assertEquals(
                DefaultNamespaceMapper.class.getCanonicalName(),
                createSinkConfig().getString(NAMESPACE_MAPPER_CONFIG)),
        () -> assertInvalid(NAMESPACE_MAPPER_CONFIG, "not a class format"),
        () -> assertInvalid(NAMESPACE_MAPPER_CONFIG, "com.example.kafka.test.NamespaceMapper"));
  }

  @TestFactory
  @DisplayName("test build multiple collection specific valid post processor chains")
  Collection<DynamicTest> buildMultipleCollectionSpecificValidPostProcessorChains() {
    List<DynamicTest> tests = new ArrayList<>();
    Map<String, String> chainDefinitions =
        new HashMap<String, String>() {
          {
            put("topic-1", "");
            put(
                "topic-2",
                format(
                    "%s,%s",
                    DocumentIdAdder.class.getName(), BlacklistValueProjector.class.getName()));
            put(
                "topic-3",
                format(
                    "%s,%s",
                    RenameByMapping.class.getName(), WhitelistKeyProjector.class.getName()));
            put(
                "topic-4",
                format(
                    "%s,%s,%s",
                    RenameByMapping.class.getName(),
                    DocumentIdAdder.class.getName(),
                    WhitelistKeyProjector.class.getName()));
          }
        };

    Map<String, List<Class>> expected =
        new HashMap<String, List<Class>>() {
          {
            put("topic-1", singletonList(DocumentIdAdder.class));
            put("topic-2", asList(DocumentIdAdder.class, BlacklistValueProjector.class));
            put(
                "topic-3",
                asList(DocumentIdAdder.class, RenameByMapping.class, WhitelistKeyProjector.class));
            put(
                "topic-4",
                asList(RenameByMapping.class, DocumentIdAdder.class, WhitelistKeyProjector.class));
          }
        };

    Map<String, String> map = createConfigMap(TOPICS_CONFIG, String.join(",", expected.keySet()));
    chainDefinitions.forEach(
        (key, value) ->
            map.put(
                format(TOPIC_OVERRIDE_CONFIG, key, POST_PROCESSOR_CHAIN_CONFIG),
                String.join(",", value)));

    MongoSinkConfig mongoSinkConfig = new MongoSinkConfig(map);
    mongoSinkConfig.getTopics().orElse(emptyList()).stream()
        .map(mongoSinkConfig::getMongoSinkTopicConfig)
        .forEach(
            cfg ->
                tests.add(
                    dynamicTest(
                        "verify resulting chain - inspecting: " + cfg.getTopic(),
                        () -> {
                          List<Class> pp =
                              cfg.getPostProcessors().getPostProcessorList().stream()
                                  .map(PostProcessor::getClass)
                                  .collect(Collectors.toList());
                          List<Class> expectedPostProcessors = expected.get(cfg.getTopic());
                          assertEquals(
                              expectedPostProcessors.size(),
                              pp.size(),
                              "chain " + cfg.getTopic() + " has wrong size");
                          assertEquals(expectedPostProcessors, pp);
                        })));
    return tests;
  }

  @TestFactory
  @DisplayName("test get single valid write model strategy")
  Collection<DynamicTest> testGetSingleValidWriteModelStrategy() {
    List<DynamicTest> tests = new ArrayList<>();

    HashMap<String, Class> candidates =
        new HashMap<String, Class>() {
          {
            put("", ReplaceOneDefaultStrategy.class);
            put(DeleteOneDefaultStrategy.class.getName(), DeleteOneDefaultStrategy.class);
            put(ReplaceOneBusinessKeyStrategy.class.getName(), ReplaceOneBusinessKeyStrategy.class);
            put(ReplaceOneDefaultStrategy.class.getName(), ReplaceOneDefaultStrategy.class);
            put(UpdateOneTimestampsStrategy.class.getName(), UpdateOneTimestampsStrategy.class);
            put(
                UpdateOneBusinessKeyTimestampStrategy.class.getName(),
                UpdateOneBusinessKeyTimestampStrategy.class);
          }
        };

    candidates.forEach(
        (key, value) -> {
          Map<String, String> map = createConfigMap();
          if (!key.isEmpty()) {
            map.put(WRITEMODEL_STRATEGY_CONFIG, key);
          }
          MongoSinkConfig cfg = new MongoSinkConfig(map);
          WriteModelStrategy wms = cfg.getMongoSinkTopicConfig(TEST_TOPIC).getWriteModelStrategy();
          tests.add(
              dynamicTest(
                  key.isEmpty()
                      ? "check write model strategy for default config"
                      : "check write model strategy for config "
                          + WRITEMODEL_STRATEGY_CONFIG
                          + "="
                          + key,
                  () ->
                      assertAll(
                          "check for non-null and correct type",
                          () -> assertNotNull(wms, "write model strategy was null"),
                          () ->
                              assertTrue(
                                  value.isInstance(wms),
                                  "write model strategy NOT of type " + value.getName()))));
        });

    return tests;
  }

  @Test
  @DisplayName("test get multiple collection specific valid write model strategies")
  void testGetMultipleCollectionSpecificValidWriteModelStrategy() {
    Map<String, Class> candidates =
        new HashMap<String, Class>() {
          {
            put("topic-1", ReplaceOneDefaultStrategy.class);
            put("topic-2", ReplaceOneBusinessKeyStrategy.class);
            put("topic-3", UpdateOneTimestampsStrategy.class);
            put("topic-5", UpdateOneBusinessKeyTimestampStrategy.class);
            put("topic-4", DeleteOneDefaultStrategy.class);
          }
        };

    Map<String, String> map = createConfigMap(TOPICS_CONFIG, String.join(",", candidates.keySet()));
    candidates.forEach(
        (topic, clazz) ->
            map.put(
                format(TOPIC_OVERRIDE_CONFIG, topic, WRITEMODEL_STRATEGY_CONFIG), clazz.getName()));

    MongoSinkConfig mongoSinkConfig = new MongoSinkConfig(map);
    mongoSinkConfig.getTopics().orElse(emptyList()).stream()
        .map(mongoSinkConfig::getMongoSinkTopicConfig)
        .forEach(
            cfg ->
                assertEquals(
                    candidates.get(cfg.getTopic()),
                    cfg.getWriteModelStrategy().getClass(),
                    "write model for "
                        + cfg.getTopic()
                        + " strategy NOT of type "
                        + candidates.get(cfg.getTopic())));
  }

  private Exception assertInvalid(final String key, final String value) {
    return assertInvalid(key, createConfigMap(key, value));
  }

  private Exception assertInvalid(final String invalidKey, final Map<String, String> configMap) {
    assertFalse(
        MongoSinkConfig.CONFIG.validateAll(configMap).get(invalidKey).errorMessages().isEmpty());
    return assertThrows(ConfigException.class, () -> new MongoSinkConfig(configMap));
  }

  private void assertPattern(final String expected, final Pattern actual) {
    assertEquals(expected, actual.pattern());
  }
}
