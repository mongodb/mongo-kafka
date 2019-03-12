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

package com.mongodb.kafka.connect;

import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.FIELD_LIST_SPLIT_CHAR;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_CHANGE_DATA_CAPTURE_HANDLER;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_CONNECTION_URI_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_DATABASE_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGIES_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_POST_PROCESSOR_CHAIN;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.MONGODB_WRITEMODEL_STRATEGY;
import static com.mongodb.kafka.connect.MongoDbSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.mongodb.ConnectionString;

import com.mongodb.kafka.connect.cdc.CdcHandler;
import com.mongodb.kafka.connect.cdc.debezium.mongodb.MongoDbHandler;
import com.mongodb.kafka.connect.cdc.debezium.rdbms.mysql.MysqlHandler;
import com.mongodb.kafka.connect.cdc.debezium.rdbms.postgres.PostgresHandler;
import com.mongodb.kafka.connect.processor.BlacklistValueProjector;
import com.mongodb.kafka.connect.processor.DocumentIdAdder;
import com.mongodb.kafka.connect.processor.PostProcessor;
import com.mongodb.kafka.connect.processor.WhitelistKeyProjector;
import com.mongodb.kafka.connect.processor.WhitelistValueProjector;
import com.mongodb.kafka.connect.processor.field.renaming.RegExpSettings;
import com.mongodb.kafka.connect.processor.field.renaming.RenameByMapping;
import com.mongodb.kafka.connect.processor.field.renaming.RenameByRegExp;
import com.mongodb.kafka.connect.writemodel.strategy.DeleteOneDefaultStrategy;
import com.mongodb.kafka.connect.writemodel.strategy.ReplaceOneBusinessKeyStrategy;
import com.mongodb.kafka.connect.writemodel.strategy.ReplaceOneDefaultStrategy;
import com.mongodb.kafka.connect.writemodel.strategy.UpdateOneTimestampsStrategy;
import com.mongodb.kafka.connect.writemodel.strategy.WriteModelStrategy;

@RunWith(JUnitPlatform.class)
class MongoDbSinkConnectorConfigTest {

    private static final String CLIENT_URI_DEFAULT_SETTINGS = "mongodb://localhost:27017";
    private static final String CLIENT_URI_AUTH_SETTINGS = "mongodb://user:pass@localhost:27017/kafkaconnect";

    Stream<String> validClassNames() {
        return Stream.of("a.b.c", "_some_weird_classname", "com.foo.Bar$Baz", "$OK");
    }

    Stream<String> inValidClassNames() {
        return Stream.of("123a.b.c", "!No", "+-");
    }

    @Test
    @DisplayName("build config doc (no test)")
    //CHECKSTYLE:OFF
    void doc() {
        System.out.println(MongoDbSinkConnectorConfig.conf().toRst());
        assertTrue(true);
    }
    //CHECKSTYLE:ON

    @Test
    @DisplayName("test build client uri for default settings")
    void testBuildClientUriWithDefaultSettings() {
        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(new HashMap<>());
        ConnectionString uri = cfg.getConnectionString();
        assertEquals(CLIENT_URI_DEFAULT_SETTINGS, uri.toString(), "wrong connection uri");
    }

    @Test
    @DisplayName("test build client uri for configured auth settings")
    void testBuildClientUriWithAuthSettings() {
        Map<String, String> map = new HashMap<>();
        map.put(MONGODB_CONNECTION_URI_CONF, CLIENT_URI_AUTH_SETTINGS);

        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
        ConnectionString uri = cfg.getConnectionString();
        assertEquals(CLIENT_URI_AUTH_SETTINGS, uri.toString(), "wrong connection uri");
    }

    @Test
    @DisplayName("test invalid client uri")
    void testInvalidURI() {
        Map<String, String> map = new HashMap<>();
        map.put(MONGODB_CONNECTION_URI_CONF, "abc");
        ConfigValue configValue = MongoDbSinkConnectorConfig.conf().validateAll(map).get(MONGODB_CONNECTION_URI_CONF);
        assertFalse(configValue.errorMessages().isEmpty());
    }

    @Test
    @DisplayName("test missing database name")
    void testMissingDatabaseName() {
        Map<String, String> map = new HashMap<>();
        map.put(MONGODB_CONNECTION_URI_CONF, "mongodb://localhost");
        map.put(MONGODB_DATABASE_CONF, "");
        ConfigValue configValue = MongoDbSinkConnectorConfig.conf().validateAll(map).get(MONGODB_DATABASE_CONF);
        assertFalse(configValue.errorMessages().isEmpty());
    }

    @Test
    @DisplayName("test K/V projection list with invalid projection type")
    void testProjectionListsForInvalidProjectionTypes() {

        assertAll("try invalid projection types for key and value list",
                () -> assertThrows(ConfigException.class, () -> {
                    HashMap<String, String> map = new HashMap<>();
                    map.put(MONGODB_KEY_PROJECTION_TYPE_CONF, "invalid");
                    new MongoDbSinkConnectorConfig(map);
                }),
                () -> assertThrows(ConfigException.class, () -> {
                    HashMap<String, String> map = new HashMap<>();
                    map.put(MONGODB_VALUE_PROJECTION_TYPE_CONF, "invalid");
                    new MongoDbSinkConnectorConfig(map);
                })
        );

    }

    @Test
    @DisplayName("test empty K/V projection field list when type 'none'")
    void testEmptyKeyValueProjectionFieldListsForNoneType() {
        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MONGODB_KEY_PROJECTION_TYPE_CONF, "none");
        map1.put(MONGODB_KEY_PROJECTION_LIST_CONF, "useless,because,ignored");
        MongoDbSinkConnectorConfig cfgKeyTypeNone = new MongoDbSinkConnectorConfig(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_VALUE_PROJECTION_TYPE_CONF, "none");
        map2.put(MONGODB_VALUE_PROJECTION_LIST_CONF, "useless,because,ignored");
        MongoDbSinkConnectorConfig cfgValueTypeNone = new MongoDbSinkConnectorConfig(map2);

        assertAll("test for empty field sets when type is none",
                () -> assertThat(cfgKeyTypeNone.getKeyProjectionList(""), CoreMatchers.is(Matchers.empty())),
                () -> assertThat(cfgValueTypeNone.getKeyProjectionList(""), CoreMatchers.is(Matchers.empty()))
        );
    }

    @Test
    @DisplayName("test correct field set for K/V projection when type is 'blacklist'")
    void testCorrectFieldSetForKeyAndValueBlacklistProjectionList() {
        String fieldList = " ,field1, field2.subA ,  field2.subB,  field3.** , ,,  ";
        HashMap<String, String> map = new HashMap<String, String>() {{
            put(MONGODB_KEY_PROJECTION_TYPE_CONF, "blacklist");
            put(MONGODB_KEY_PROJECTION_LIST_CONF, fieldList);
            put(MONGODB_VALUE_PROJECTION_TYPE_CONF, "blacklist");
            put(MONGODB_VALUE_PROJECTION_LIST_CONF, fieldList);
        }};
        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);

        Set<String> blacklisted = new HashSet<>(asList("field1", "field2.subA", "field2.subB", "field3.**"));
        assertAll("test correct field set for K/V blacklist projection",
                () -> assertThat(cfg.getKeyProjectionList(""), Matchers.containsInAnyOrder(blacklisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(""), Matchers.containsInAnyOrder(blacklisted.toArray()))
        );
    }

    @Test
    @DisplayName("test correct field set for K/V projection when type is 'whitelist'")
    void testCorrectFieldSetForKeyAndValueWhiteListProjectionList() {
        String fieldList = " ,field1.**, field2.*.subSubA ,  field2.subB.*,  field3.subC.subSubD , ,,  ";
        HashMap<String, String> map = new HashMap<String, String>() {{
            put(MONGODB_KEY_PROJECTION_TYPE_CONF, "whitelist");
            put(MONGODB_KEY_PROJECTION_LIST_CONF, fieldList);
            put(MONGODB_VALUE_PROJECTION_TYPE_CONF, "whitelist");
            put(MONGODB_VALUE_PROJECTION_LIST_CONF, fieldList);
        }};
        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);

        //this test for all entries after doing left prefix expansion which is used for whitelisting
        Set<String> whitelisted = new HashSet<>(asList("field1", "field1.**", "field2", "field2.*", "field2.*.subSubA",
                "field2.subB", "field2.subB.*", "field3", "field3.subC", "field3.subC.subSubD"));

        assertAll("test correct field set for K/V whitelist projection",
                () -> assertThat(cfg.getKeyProjectionList(""), Matchers.containsInAnyOrder(whitelisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(""), Matchers.containsInAnyOrder(whitelisted.toArray()))
        );
    }

    @TestFactory
    @DisplayName("test for (in)valid id strategies")
    Collection<DynamicTest> testForIdStrategies() {
        List<DynamicTest> modeTests = new ArrayList<>();

        for (String mode : MongoDbSinkConnectorConfig.getPredefinedIdStrategyClassNames()) {
            HashMap<String, String> map1 = new HashMap<>();
            map1.put(MONGODB_KEY_PROJECTION_TYPE_CONF, "blacklist");
            map1.put(MONGODB_DOCUMENT_ID_STRATEGY_CONF, mode);
            MongoDbSinkConnectorConfig cfgBL = new MongoDbSinkConnectorConfig(map1);

            modeTests.add(dynamicTest("blacklist: test id strategy for " + mode,
                    () -> assertThat(cfgBL.getIdStrategy("").getClass().getName(), CoreMatchers.equalTo(mode))
            ));

            HashMap<String, String> map2 = new HashMap<>();
            map2.put(MONGODB_KEY_PROJECTION_TYPE_CONF, "whitelist");
            map2.put(MONGODB_DOCUMENT_ID_STRATEGY_CONF, mode);
            MongoDbSinkConnectorConfig cfgWL = new MongoDbSinkConnectorConfig(map2);

            modeTests.add(dynamicTest("whitelist: test id strategy for " + mode,
                    () -> assertThat(cfgWL.getIdStrategy("").getClass().getName(), CoreMatchers.equalTo(mode))
            ));
        }

        return modeTests;

    }

    @TestFactory
    @DisplayName("test valid id strategy names")
    Collection<DynamicTest> testValidIdStrategyNames() {
        return Stream.concat(validClassNames()
                        .map(s -> Collections.singletonMap(MONGODB_DOCUMENT_ID_STRATEGY_CONF, s))
                        .map(m -> dynamicTest("valid id strategy: " + m.get(MONGODB_DOCUMENT_ID_STRATEGY_CONF),
                                () -> MongoDbSinkConnectorConfig.conf().validateAll(m))),
                Stream.of(dynamicTest("valid id strategies: " + validClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)),
                        () -> {
                            String v = validClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));
                            Map<String, String> m = Collections.singletonMap(MONGODB_DOCUMENT_ID_STRATEGIES_CONF, v);
                            MongoDbSinkConnectorConfig.conf().validateAll(m);
                        })))
                .collect(Collectors.toList());
    }

    @TestFactory
    @DisplayName("test invalid id strategy names")
    Collection<DynamicTest> testInvalidIdStrategyNames() {
        return Stream.concat(inValidClassNames()
                        .map(s -> Collections.singletonMap(MONGODB_DOCUMENT_ID_STRATEGY_CONF, s))
                        .map(m -> dynamicTest("invalid id strategy: " + m.get(MONGODB_DOCUMENT_ID_STRATEGY_CONF),
                                () -> assertThrows(ConfigException.class,
                                        () -> MongoDbSinkConnectorConfig.conf().validateAll(m)))),
                Stream.of(dynamicTest("invalid id strategies: " + inValidClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)),
                        () -> assertThrows(ConfigException.class, () -> {
                            String v = inValidClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));
                            Map<String, String> m = Collections.singletonMap(MONGODB_DOCUMENT_ID_STRATEGIES_CONF, v);
                            MongoDbSinkConnectorConfig.conf().validateAll(m);
                        }))))
                .collect(Collectors.toList());

    }

    @TestFactory
    @DisplayName("test valid post processor chain names")
    Collection<DynamicTest> testValidPostProcessorChainNames() {
        return Stream.concat(validClassNames()
                        .map(s -> Collections.singletonMap(MONGODB_POST_PROCESSOR_CHAIN, s))
                        .map(m -> dynamicTest("valid post processor chain: " + m.get(MONGODB_POST_PROCESSOR_CHAIN),
                                () -> MongoDbSinkConnectorConfig.conf().validateAll(m))),
                Stream.of(dynamicTest("valid post processor chain: " + validClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)),
                        () -> {
                            String v = validClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));
                            Map<String, String> m = Collections.singletonMap(MONGODB_POST_PROCESSOR_CHAIN, v);
                            MongoDbSinkConnectorConfig.conf().validateAll(m);
                        })))
                .collect(Collectors.toList());

    }

    @TestFactory
    @DisplayName("test invalid post processor chain names")
    Collection<DynamicTest> testInvalidPostProcessorChainNames() {
        return Stream.concat(inValidClassNames()
                        .map(s -> Collections.singletonMap(MONGODB_POST_PROCESSOR_CHAIN, s))
                        .map(m -> dynamicTest("invalid post processor chain: " + m.get(MONGODB_POST_PROCESSOR_CHAIN),
                                () -> assertThrows(ConfigException.class, () -> MongoDbSinkConnectorConfig.conf().validateAll(m)))),
                Stream.of(dynamicTest("invalid post processor chain: "
                                + inValidClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)),
                        () -> assertThrows(ConfigException.class, () -> {
                            String v = inValidClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));
                            Map<String, String> m = Collections.singletonMap(MONGODB_POST_PROCESSOR_CHAIN, v);
                            MongoDbSinkConnectorConfig.conf().validateAll(m);
                        }))))
                .collect(Collectors.toList());
    }

    @TestFactory
    @DisplayName("test valid change data capture handler names")
    Collection<DynamicTest> testValidChangeDataCaptureHandlerNames() {
        return validClassNames()
                .map(s -> Collections.singletonMap(MONGODB_CHANGE_DATA_CAPTURE_HANDLER, s))
                .map(m -> dynamicTest("valid change data capture handlers: " + m.get(MONGODB_CHANGE_DATA_CAPTURE_HANDLER),
                        () -> MongoDbSinkConnectorConfig.conf().validateAll(m)))
                .collect(Collectors.toList());
    }

    @TestFactory
    @DisplayName("test invalid change data capture handler names")
    Collection<DynamicTest> testInvalidChangeDataCaptureHandlerNames() {
        return inValidClassNames()
                .map(s -> Collections.singletonMap(MONGODB_CHANGE_DATA_CAPTURE_HANDLER, s))
                .map(m -> dynamicTest("invalid change data capture handlers: " + m.get(MONGODB_CHANGE_DATA_CAPTURE_HANDLER),
                        () -> assertThrows(ConfigException.class, () -> MongoDbSinkConnectorConfig.conf().validateAll(m))))
                .collect(Collectors.toList());
    }

    @TestFactory
    @DisplayName("test parse json rename field name mappings")
    Collection<DynamicTest> testParseJsonRenameFieldnameMappings() {
        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MONGODB_FIELD_RENAMER_MAPPING, "[]");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        tests.add(dynamicTest("parsing fieldname mapping for (" + cfg1.getString(MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertEquals(new HashMap<>(), cfg1.parseRenameFieldnameMappings(""))));

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_FIELD_RENAMER_MAPPING,
                "[{\"oldName\":\"key.fieldA\",\"newName\":\"field1\"},{\"oldName\":\"value.xyz\",\"newName\":\"abc\"}]");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        HashMap<String, String> result2 = new HashMap<>();
        result2.put("key.fieldA", "field1");
        result2.put("value.xyz", "abc");

        tests.add(dynamicTest("parsing fieldname mapping for (" + cfg2.getString(MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertEquals(result2, cfg2.parseRenameFieldnameMappings(""))));

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MONGODB_FIELD_RENAMER_MAPPING, "]some {invalid JSON]}[");
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);

        tests.add(dynamicTest("parsing fieldname mapping for (" + cfg3.getString(MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertThrows(ConfigException.class, () -> cfg3.parseRenameFieldnameMappings(""))));

        return tests;

    }

    @TestFactory
    @DisplayName("test parse json rename regexp settings")
    Collection<DynamicTest> testParseJsonRenameRegExpSettings() {

        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MONGODB_FIELD_RENAMER_REGEXP, "[]");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        tests.add(dynamicTest("parsing regexp settings for (" + cfg1.getString(MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertEquals(emptyList(), cfg1.parseRenameRegExpSettings(""))));

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_FIELD_RENAMER_REGEXP,
                "[{\"regexp\":\"^key\\\\..*my.*$\",\"pattern\":\"my\",\"replace\":\"\"},"
                        + "{\"regexp\":\"^value\\\\..*$\",\"pattern\":\"\\\\.\",\"replace\":\"_\"}]");

        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        List<RegExpSettings> result2 = new ArrayList<>();
        result2.add(new RegExpSettings("^key\\..*my.*$", "my", ""));
        result2.add(new RegExpSettings("^value\\..*$", "\\.", "_"));

        tests.add(dynamicTest("parsing regexp settings for (" + cfg2.getString(MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertEquals(result2, cfg2.parseRenameRegExpSettings(""))));

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MONGODB_FIELD_RENAMER_REGEXP, "]some {invalid JSON]}[");
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);

        tests.add(dynamicTest("parsing regexp settings for (" + cfg3.getString(MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertThrows(ConfigException.class, () -> cfg3.parseRenameRegExpSettings(""))));

        return tests;

    }

    @TestFactory
    @DisplayName("test build single valid post processor chain")
    Collection<DynamicTest> testBuildSingleValidPostProcessorChain() {
        List<DynamicTest> tests = new ArrayList<>();

        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(new HashMap<>());
        PostProcessor chain1 = cfg1.buildPostProcessorChain("");
        tests.add(dynamicTest("check chain result if not specified and using default", () ->
                assertAll("check for type and no successor",
                        () -> assertTrue(chain1 instanceof DocumentIdAdder,
                                "post processor not of type " + DocumentIdAdder.class.getName()),
                        () -> assertEquals(Optional.empty(), chain1.getNext()))
        ));

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_POST_PROCESSOR_CHAIN, "");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        PostProcessor chain2 = cfg2.buildPostProcessorChain("");
        tests.add(dynamicTest("check chain result if specified to be empty", () ->
                assertAll("check for type and no successor",
                        () -> assertTrue(chain2 instanceof DocumentIdAdder,
                                "post processor not of type " + DocumentIdAdder.class.getName()),
                        () -> assertEquals(Optional.empty(), chain2.getNext()))
        ));

        List<Class> classes = new ArrayList<>();
        classes.add(DocumentIdAdder.class);
        classes.add(BlacklistValueProjector.class);
        classes.add(WhitelistValueProjector.class);
        classes.add(RenameByMapping.class);
        classes.add(RenameByRegExp.class);

        String processors = classes.stream().map(Class::getName)
                .collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MONGODB_POST_PROCESSOR_CHAIN, processors);
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);
        PostProcessor chain3 = cfg3.buildPostProcessorChain("");

        tests.add(dynamicTest("check chain result for full config: " + processors, () -> {
                    PostProcessor pp = chain3;
                    for (Class clazz : classes) {
                        assertEquals(clazz, pp.getClass());
                        if (pp.getNext().isPresent()) {
                            pp = pp.getNext().get();
                        }
                    }
                    assertEquals(Optional.empty(), pp.getNext());
                }
        ));

        Class docIdAdder = classes.remove(0);

        processors = classes.stream().map(Class::getName).collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));

        classes.add(0, docIdAdder);

        HashMap<String, String> map4 = new HashMap<>();
        map4.put(MONGODB_POST_PROCESSOR_CHAIN, processors);
        MongoDbSinkConnectorConfig cfg4 = new MongoDbSinkConnectorConfig(map4);
        PostProcessor chain4 = cfg4.buildPostProcessorChain("");

        tests.add(dynamicTest("check chain result for config missing DocumentIdAdder: " + processors, () -> {
                    PostProcessor pp = chain4;
                    for (Class clazz : classes) {
                        assertEquals(clazz, pp.getClass());
                        if (pp.getNext().isPresent()) {
                            pp = pp.getNext().get();
                        }
                    }
                    assertEquals(Optional.empty(), pp.getNext());
                }
        ));

        return tests;
    }

    @Test
    @DisplayName("test build single invalid post processor chain")
    void testBuildSingleInvalidPostProcessorChain() {
        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MONGODB_POST_PROCESSOR_CHAIN, "com.mongodb.kafka.class.NotExisting");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_POST_PROCESSOR_CHAIN, "com.mongodb.kafka.connect.MongoDbSinkTask");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        assertAll("check for not existing and invalid class used for post processor",
                () -> assertThrows(ConfigException.class, () -> cfg1.buildPostProcessorChain("")),
                () -> assertThrows(ConfigException.class, () -> cfg2.buildPostProcessorChain("")));
    }

    @TestFactory
    @DisplayName("test build multiple collection specific valid post processor chains")
    Collection<DynamicTest> buildMultipleCollectionSpecificValidPostProcessorChains() {
        List<DynamicTest> tests = new ArrayList<>();

        Map<String, List<Class>> chainDefinitions = new HashMap<String, List<Class>>() {{
            put("collection-1", new ArrayList<>());
            put("collection-2", asList(DocumentIdAdder.class, BlacklistValueProjector.class));
            put("collection-3", asList(RenameByMapping.class, WhitelistKeyProjector.class));
        }};

        Map<String, List<Class>> chainResults = new HashMap<String, List<Class>>() {{
            put(TOPIC_AGNOSTIC_KEY_NAME, singletonList(DocumentIdAdder.class));
            put("collection-1", singletonList(DocumentIdAdder.class));
            put("collection-2", asList(DocumentIdAdder.class, BlacklistValueProjector.class));
            put("collection-3", asList(DocumentIdAdder.class, RenameByMapping.class, WhitelistKeyProjector.class));
        }};

        HashMap<String, String> map = new HashMap<>();
        List<String> collections = new ArrayList<>();

        chainDefinitions.forEach((key, value) -> {
            collections.add(key);
            map.put(MONGODB_POST_PROCESSOR_CHAIN + "." + key,
                    value.stream().map(Class::getName).collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)));
        });

        map.put(MONGODB_COLLECTIONS_CONF, String.join(FIELD_LIST_SPLIT_CHAR, collections));

        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
        Map<String, PostProcessor> builtChains = cfg.buildPostProcessorChains();

        assertEquals(chainDefinitions.size() + 1, builtChains.size(), "wrong number of created chains");

        //NOTE: verify the creation of the fallback chain __default__ when nothing was defined
        collections.add(TOPIC_AGNOSTIC_KEY_NAME);

        collections.forEach(c ->
                tests.add(dynamicTest("verify resulting chain - inspecting: " + c, () ->
                        assertAll("inspecting chain for " + c,
                                () -> assertTrue(builtChains.containsKey(c), "must contain chain for " + c),
                                () -> {
                                    PostProcessor pp = builtChains.get(c);
                                    int length = 1;
                                    for (Class clazz : chainResults.get(c)) {
                                        assertEquals(clazz, pp.getClass());
                                        if (pp.getNext().isPresent()) {
                                            pp = pp.getNext().get();
                                            length++;
                                        }
                                    }
                                    assertEquals(Optional.empty(), pp.getNext());
                                    assertEquals(chainResults.get(c).size(), length, "chain " + c + " has wrong size");
                                }
                        )
                )));

        return tests;

    }

    @TestFactory
    @DisplayName("test get single valid write model strategy")
    Collection<DynamicTest> testGetSingleValidWriteModelStrategy() {
        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, Class> candidates = new HashMap<String, Class>() {{
            put("", ReplaceOneDefaultStrategy.class);
            put(DeleteOneDefaultStrategy.class.getName(), DeleteOneDefaultStrategy.class);
            put(ReplaceOneBusinessKeyStrategy.class.getName(), ReplaceOneBusinessKeyStrategy.class);
            put(ReplaceOneDefaultStrategy.class.getName(), ReplaceOneDefaultStrategy.class);
            put(UpdateOneTimestampsStrategy.class.getName(), UpdateOneTimestampsStrategy.class);
        }};

        candidates.forEach((key, value) -> {
            HashMap<String, String> map = new HashMap<>();
            if (!key.isEmpty()) {
                map.put(MONGODB_WRITEMODEL_STRATEGY, key);
            }
            MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
            WriteModelStrategy wms = cfg.getWriteModelStrategy("");
            tests.add(dynamicTest(key.isEmpty() ? "check write model strategy for default config"
                            : "check write model strategy for config " + MONGODB_WRITEMODEL_STRATEGY + "=" + key,
                    () -> assertAll("check for non-null and correct type",
                            () -> assertNotNull(wms, "write model strategy was null"),
                            () -> assertTrue(value.isInstance(wms), "write model strategy NOT of type " + value.getName()))
            ));
        });

        return tests;
    }

    @TestFactory
    @DisplayName("test get multiple collection specific valid write model strategies")
    Collection<DynamicTest> testGetMultipleCollectionSpecificValidWriteModelStrategy() {
        List<DynamicTest> tests = new ArrayList<>();

        Map<String, Class> canditates = new HashMap<String, Class>() {{
            put("", ReplaceOneDefaultStrategy.class);
            put("collection-1", ReplaceOneDefaultStrategy.class);
            put("collection-2", ReplaceOneBusinessKeyStrategy.class);
            put("collection-3", UpdateOneTimestampsStrategy.class);
            put("collection-4", DeleteOneDefaultStrategy.class);
        }};

        HashMap<String, String> map = new HashMap<>();
        canditates.forEach((key1, value1) -> map.put(MONGODB_WRITEMODEL_STRATEGY + "." + key1, value1.getName()));
        map.put(MONGODB_COLLECTIONS_CONF, String.join(FIELD_LIST_SPLIT_CHAR, canditates.keySet()));

        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
        Map<String, WriteModelStrategy> wms = cfg.getWriteModelStrategies();

        wms.forEach((key, value) -> tests.add(dynamicTest(
                TOPIC_AGNOSTIC_KEY_NAME.equals(key)
                        ? "check fallback write model strategy for config " + MONGODB_WRITEMODEL_STRATEGY
                        : "check write model strategy for config " + MONGODB_WRITEMODEL_STRATEGY + "." + key,
                () -> assertAll("check for non-null and correct type",
                        () -> assertNotNull(value, "write model strategy was null"),
                        () -> assertTrue(canditates.get(TOPIC_AGNOSTIC_KEY_NAME.equals(key) ? "" : key).isInstance(value),
                                "write model strategy NOT of type " + canditates.get(key))))
        ));

        return tests;
    }

    @TestFactory
    @DisplayName("test get single valid cdc handler")
    Collection<DynamicTest> testGetSingleValidCdcHandler() {
        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, Class> candidates = new HashMap<String, Class>() {{
            put(MongoDbHandler.class.getName(), MongoDbHandler.class);
            put(MysqlHandler.class.getName(), MysqlHandler.class);
            put(PostgresHandler.class.getName(), PostgresHandler.class);
        }};

        candidates.forEach((key, value) -> {
            HashMap<String, String> map = new HashMap<>();
            map.put(MONGODB_CHANGE_DATA_CAPTURE_HANDLER, key);
            MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
            CdcHandler cdc = cfg.getCdcHandler("");
            tests.add(dynamicTest("check cdc handler for config" + MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "=" + key,
                    () -> assertAll("check for non-null and correct type",
                            () -> assertNotNull(cdc, "cdc handler was null"),
                            () -> assertTrue(value.isInstance(cdc), "cdc handler NOT of type " + value.getName()))
            ));
        });

        tests.add(dynamicTest("check cdc handler for config" + MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "=",
                () -> assertNull(new MongoDbSinkConnectorConfig(new HashMap<>()).getCdcHandler(""), "cdc handler was not null")));

        return tests;
    }

    @TestFactory
    @DisplayName("test get multiple collection specific valid cdc handlers")
    Collection<DynamicTest> testGetMultipleCollectionSpecificValidCdcHandlers() {
        List<DynamicTest> tests = new ArrayList<>();

        Map<String, Class> canditates = new HashMap<String, Class>() {{
            put("collection-1", MongoDbHandler.class);
            put("collection-2", MysqlHandler.class);
            put("collection-3", PostgresHandler.class);
        }};

        HashMap<String, String> map = new HashMap<>();
        canditates.forEach((key1, value1) -> map.put(MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "." + key1, value1.getName()));
        map.put(MONGODB_COLLECTIONS_CONF, String.join(FIELD_LIST_SPLIT_CHAR, canditates.keySet()));

        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
        Map<String, CdcHandler> cdc = cfg.getCdcHandlers();

        cdc.forEach((key, value) -> tests.add(dynamicTest("check cdc handler for config " + MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "." + key,
                () -> assertAll("check for non-null and correct type",
                        () -> assertNotNull(value, "cdc handler was null"),
                        () -> assertTrue(canditates.get(key).isInstance(value), "cdc handler NOT of type " + canditates.get(key))
                ))
        ));

        return tests;
    }
}
