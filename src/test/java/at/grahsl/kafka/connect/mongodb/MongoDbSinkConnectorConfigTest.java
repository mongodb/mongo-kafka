/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2017 Hans-Peter Grahsl.
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

package at.grahsl.kafka.connect.mongodb;

import at.grahsl.kafka.connect.mongodb.cdc.CdcHandler;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb.MongoDbHandler;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms.mysql.MysqlHandler;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.rdbms.postgres.PostgresHandler;
import at.grahsl.kafka.connect.mongodb.processor.BlacklistValueProjector;
import at.grahsl.kafka.connect.mongodb.processor.DocumentIdAdder;
import at.grahsl.kafka.connect.mongodb.processor.PostProcessor;
import at.grahsl.kafka.connect.mongodb.processor.WhitelistKeyProjector;
import at.grahsl.kafka.connect.mongodb.processor.WhitelistValueProjector;
import at.grahsl.kafka.connect.mongodb.processor.field.renaming.RenameByMapping;
import at.grahsl.kafka.connect.mongodb.processor.field.renaming.RenameByRegExp;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.DeleteOneDefaultStrategy;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.ReplaceOneBusinessKeyStrategy;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.ReplaceOneDefaultStrategy;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.UpdateOneTimestampsStrategy;
import at.grahsl.kafka.connect.mongodb.writemodel.strategy.WriteModelStrategy;
import com.mongodb.MongoClientURI;
import org.apache.kafka.common.config.ConfigException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
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

import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.FIELD_LIST_SPLIT_CHAR;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_CHANGE_DATA_CAPTURE_HANDLER;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_COLLECTIONS_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_CONNECTION_URI_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGIES_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_POST_PROCESSOR_CHAIN;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.MONGODB_WRITEMODEL_STRATEGY;
import static at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig.TOPIC_AGNOSTIC_KEY_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

@RunWith(JUnitPlatform.class)
public class MongoDbSinkConnectorConfigTest {

    public static final String CLIENT_URI_DEFAULT_SETTINGS =
            "mongodb://localhost:27017/kafkaconnect?w=1&journal=true";
    public static final String CLIENT_URI_AUTH_SETTINGS =
            "mongodb://hanspeter:secret@localhost:27017/kafkaconnect?w=1&journal=true&authSource=admin&authMechanism=SCRAM-SHA-1";

    public Stream<String> validClassNames() {
        return Stream.of("a.b.c",
                "_some_weird_classname",
                "com.foo.Bar$Baz",
                "$OK");
    }

    public Stream<String> inValidClassNames() {
        return Stream.of("123a.b.c",
                "!No",
                "+-");
    }

    @Test
    @DisplayName("build config doc (no test)")
    public void doc() {
        System.out.println(MongoDbSinkConnectorConfig.conf().toRst());
        assertTrue(true);
    }

    @Test
    @DisplayName("test build client uri for default settings")
    public void testBuildClientUriWithDefaultSettings() {

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(new HashMap<>());

        MongoClientURI uri = cfg.buildClientURI();

        assertEquals(CLIENT_URI_DEFAULT_SETTINGS, uri.toString(), "wrong connection uri");

    }

    @Test
    @DisplayName("test build client uri for configured auth settings")
    public void testBuildClientUriWithAuthSettings() {

        Map<String, String> map = new HashMap<String, String>();
        map.put(MONGODB_CONNECTION_URI_CONF,
                "mongodb://hanspeter:secret@localhost:27017/kafkaconnect?w=1&journal=true&authSource=admin&authMechanism=SCRAM-SHA-1");

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(map);

        MongoClientURI uri = cfg.buildClientURI();

        assertEquals(CLIENT_URI_AUTH_SETTINGS, uri.toString(), "wrong connection uri");

    }

    @Test
    @DisplayName("test K/V projection list with invalid projection type")
    public void testProjectionListsForInvalidProjectionTypes() {

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
    public void testEmptyKeyValueProjectionFieldListsForNoneType() {
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
    public void testCorrectFieldSetForKeyAndValueBlacklistProjectionList() {
        String fieldList = " ,field1, field2.subA ,  field2.subB,  field3.** , ,,  ";
        HashMap<String, String> map = new HashMap<>();
        map.put(MONGODB_KEY_PROJECTION_TYPE_CONF, "blacklist");
        map.put(MONGODB_KEY_PROJECTION_LIST_CONF,
                fieldList);
        map.put(MONGODB_VALUE_PROJECTION_TYPE_CONF, "blacklist");
        map.put(MONGODB_VALUE_PROJECTION_LIST_CONF,
                fieldList);
        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);

        Set<String> blacklisted = new HashSet<>();
        blacklisted.addAll(Arrays.asList("field1", "field2.subA", "field2.subB", "field3.**"));

        assertAll("test correct field set for K/V blacklist projection",
                () -> assertThat(cfg.getKeyProjectionList(""), Matchers.containsInAnyOrder(blacklisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(""), Matchers.containsInAnyOrder(blacklisted.toArray()))
        );
    }

    @Test
    @DisplayName("test correct field set for K/V projection when type is 'whitelist'")
    public void testCorrectFieldSetForKeyAndValueWhiteListProjectionList() {
        String fieldList = " ,field1.**, field2.*.subSubA ,  field2.subB.*,  field3.subC.subSubD , ,,  ";
        HashMap<String, String> map = new HashMap<>();
        map.put(MONGODB_KEY_PROJECTION_TYPE_CONF, "whitelist");
        map.put(MONGODB_KEY_PROJECTION_LIST_CONF,
                fieldList);
        map.put(MONGODB_VALUE_PROJECTION_TYPE_CONF, "whitelist");
        map.put(MONGODB_VALUE_PROJECTION_LIST_CONF,
                fieldList);
        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);

        //this test for all entries after doing left prefix expansion which is used for whitelisting
        Set<String> whitelisted = new HashSet<String>();
        whitelisted.addAll(Arrays.asList("field1", "field1.**",
                "field2", "field2.*", "field2.*.subSubA",
                "field2.subB", "field2.subB.*",
                "field3", "field3.subC", "field3.subC.subSubD"));

        assertAll("test correct field set for K/V whitelist projection",
                () -> assertThat(cfg.getKeyProjectionList(""), Matchers.containsInAnyOrder(whitelisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(""), Matchers.containsInAnyOrder(whitelisted.toArray()))
        );

    }

    @TestFactory
    @DisplayName("test for (in)valid id strategies")
    public Collection<DynamicTest> testForIdStrategies() {

        List<DynamicTest> modeTests = new ArrayList<>();

        for (String mode :
                MongoDbSinkConnectorConfig.getPredefinedIdStrategyClassNames()) {

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
    public Collection<DynamicTest> testValidIdStrategyNames() {
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
    public Collection<DynamicTest> testInvalidIdStrategyNames() {
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
    public Collection<DynamicTest> testValidPostProcessorChainNames() {
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
    public Collection<DynamicTest> testInvalidPostProcessorChainNames() {
        return Stream.concat(inValidClassNames()
                        .map(s -> Collections.singletonMap(MONGODB_POST_PROCESSOR_CHAIN, s))
                        .map(m -> dynamicTest("invalid post processor chain: " + m.get(MONGODB_POST_PROCESSOR_CHAIN),
                                () -> assertThrows(ConfigException.class, () -> {
                                    MongoDbSinkConnectorConfig.conf().validateAll(m);
                                }))),
                Stream.of(dynamicTest("invalid post processor chain: " + inValidClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)),
                        () -> assertThrows(ConfigException.class, () -> {
                            String v = inValidClassNames().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));
                            Map<String, String> m = Collections.singletonMap(MONGODB_POST_PROCESSOR_CHAIN, v);
                            MongoDbSinkConnectorConfig.conf().validateAll(m);
                        }))))
                .collect(Collectors.toList());
    }

    @TestFactory
    @DisplayName("test valid change data capture handler names")
    public Collection<DynamicTest> testValidChangeDataCaptureHandlerNames() {
        return validClassNames()
                .map(s -> Collections.singletonMap(MONGODB_CHANGE_DATA_CAPTURE_HANDLER, s))
                .map(m -> dynamicTest("valid change data capture handlers: " + m.get(MONGODB_CHANGE_DATA_CAPTURE_HANDLER),
                        () -> MongoDbSinkConnectorConfig.conf().validateAll(m)))
                .collect(Collectors.toList());
    }

    @TestFactory
    @DisplayName("test invalid change data capture handler names")
    public Collection<DynamicTest> testInvalidChangeDataCaptureHandlerNames() {
        return inValidClassNames()
                .map(s -> Collections.singletonMap(MONGODB_CHANGE_DATA_CAPTURE_HANDLER, s))
                .map(m -> dynamicTest("invalid change data capture handlers: " + m.get(MONGODB_CHANGE_DATA_CAPTURE_HANDLER),
                        () -> assertThrows(ConfigException.class, () -> {
                            MongoDbSinkConnectorConfig.conf().validateAll(m);
                        })))
                .collect(Collectors.toList());
    }

    @TestFactory
    @DisplayName("test parse json rename field name mappings")
    public Collection<DynamicTest> testParseJsonRenameFieldnameMappings() {

        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MONGODB_FIELD_RENAMER_MAPPING, "[]");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        tests.add(dynamicTest("parsing fieldname mapping for (" +
                        cfg1.getString(MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertEquals(new HashMap<>(), cfg1.parseRenameFieldnameMappings("")))
        );

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_FIELD_RENAMER_MAPPING,
                "[{\"oldName\":\"key.fieldA\",\"newName\":\"field1\"},{\"oldName\":\"value.xyz\",\"newName\":\"abc\"}]");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        HashMap<String, String> result2 = new HashMap<>();
        result2.put("key.fieldA", "field1");
        result2.put("value.xyz", "abc");

        tests.add(dynamicTest("parsing fieldname mapping for (" +
                        cfg2.getString(MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertEquals(result2, cfg2.parseRenameFieldnameMappings("")))
        );

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MONGODB_FIELD_RENAMER_MAPPING, "]some {invalid JSON]}[");
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);

        tests.add(dynamicTest("parsing fieldname mapping for (" +
                        cfg3.getString(MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertThrows(ConfigException.class, () -> cfg3.parseRenameFieldnameMappings("")))
        );

        return tests;

    }

    @TestFactory
    @DisplayName("test parse json rename regexp settings")
    public Collection<DynamicTest> testParseJsonRenameRegExpSettings() {

        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MONGODB_FIELD_RENAMER_REGEXP, "[]");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        tests.add(dynamicTest("parsing regexp settings for (" +
                        cfg1.getString(MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertEquals(new HashMap<>(), cfg1.parseRenameRegExpSettings("")))
        );

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_FIELD_RENAMER_REGEXP,
                "[{\"regexp\":\"^key\\\\..*my.*$\",\"pattern\":\"my\",\"replace\":\"\"}," +
                        "{\"regexp\":\"^value\\\\..*$\",\"pattern\":\"\\\\.\",\"replace\":\"_\"}]");

        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        HashMap<String, RenameByRegExp.PatternReplace> result2 = new HashMap<>();
        result2.put("^key\\..*my.*$", new RenameByRegExp.PatternReplace("my", ""));
        result2.put("^value\\..*$", new RenameByRegExp.PatternReplace("\\.", "_"));

        tests.add(dynamicTest("parsing regexp settings for (" +
                        cfg2.getString(MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertEquals(result2, cfg2.parseRenameRegExpSettings("")))
        );

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MONGODB_FIELD_RENAMER_REGEXP, "]some {invalid JSON]}[");
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);

        tests.add(dynamicTest("parsing regexp settings for (" +
                        cfg3.getString(MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertThrows(ConfigException.class, () -> cfg3.parseRenameRegExpSettings("")))
        );

        return tests;

    }

    @TestFactory
    @DisplayName("test build single valid post processor chain")
    public Collection<DynamicTest> testBuildSingleValidPostProcessorChain() {

        List<DynamicTest> tests = new ArrayList<>();

        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(new HashMap<>());
        PostProcessor chain1 = cfg1.buildPostProcessorChain("");
        tests.add(dynamicTest("check chain result if not specified and using default", () ->
                assertAll("check for type and no successor",
                        () -> assertTrue(chain1 instanceof DocumentIdAdder,
                                "post processor not of type " + DocumentIdAdder.class.getName()),
                        () -> assertEquals(Optional.empty(), chain1.getNext())
                )
        ));

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_POST_PROCESSOR_CHAIN, "");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        PostProcessor chain2 = cfg2.buildPostProcessorChain("");
        tests.add(dynamicTest("check chain result if specified to be empty", () ->
                assertAll("check for type and no successor",
                        () -> assertTrue(chain2 instanceof DocumentIdAdder,
                                "post processor not of type " + DocumentIdAdder.class.getName()),
                        () -> assertEquals(Optional.empty(), chain2.getNext())
                )
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

        processors = classes.stream().map(Class::getName)
                .collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR));

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
    public void testBuildSingleInvalidPostProcessorChain() {
        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MONGODB_POST_PROCESSOR_CHAIN,
                "at.grahsl.class.NotExisting");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MONGODB_POST_PROCESSOR_CHAIN,
                "at.grahsl.kafka.connect.mongodb.MongoDbSinkTask");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        assertAll("check for not existing and invalid class used for post processor",
                () -> assertThrows(ConfigException.class, () -> cfg1.buildPostProcessorChain("")),
                () -> assertThrows(ConfigException.class, () -> cfg2.buildPostProcessorChain(""))
        );
    }

    @TestFactory
    @DisplayName("test build multiple collection specific valid post processor chains")
    public Collection<DynamicTest> buildMultipleCollectionSpecificValidPostProcessorChains() {

        List<DynamicTest> tests = new ArrayList<>();

        Map<String, List<Class>> chainDefinitions = new HashMap<String, List<Class>>() {{
            put("collection-1",
                    new ArrayList<>());
            put("collection-2",
                    Arrays.asList(DocumentIdAdder.class, BlacklistValueProjector.class));
            put("collection-3",
                    Arrays.asList(RenameByMapping.class, WhitelistKeyProjector.class));
        }};

        Map<String, List<Class>> chainResults = new HashMap<String, List<Class>>() {{
            put(TOPIC_AGNOSTIC_KEY_NAME,
                    Arrays.asList(DocumentIdAdder.class));
            put("collection-1",
                    Arrays.asList(DocumentIdAdder.class));
            put("collection-2",
                    Arrays.asList(DocumentIdAdder.class, BlacklistValueProjector.class));
            put("collection-3",
                    Arrays.asList(DocumentIdAdder.class, RenameByMapping.class, WhitelistKeyProjector.class));
        }};

        HashMap<String, String> map = new HashMap<>();

        List<String> collections = new ArrayList<>();

        chainDefinitions.entrySet().forEach(entry -> {
                    collections.add(entry.getKey());
                    map.put(MONGODB_POST_PROCESSOR_CHAIN + "." + entry.getKey(),
                            entry.getValue().stream().map(Class::getName)
                                    .collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)));
                }
        );

        map.put(MONGODB_COLLECTIONS_CONF,
                collections.stream().collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)));

        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
        Map<String, PostProcessor> builtChains = cfg.buildPostProcessorChains();

        assertEquals(chainDefinitions.size() + 1, builtChains.size(), "wrong number of created chains");

        //NOTE: verify the creation of the fallback chain __default__ when nothing was defined
        collections.add(TOPIC_AGNOSTIC_KEY_NAME);

        collections.stream().forEach(c ->
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
    public Collection<DynamicTest> testGetSingleValidWriteModelStrategy() {

        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, Class> candidates = new HashMap<String, Class>() {{
            put("", ReplaceOneDefaultStrategy.class);
            put(DeleteOneDefaultStrategy.class.getName(), DeleteOneDefaultStrategy.class);
            put(ReplaceOneBusinessKeyStrategy.class.getName(), ReplaceOneBusinessKeyStrategy.class);
            put(ReplaceOneDefaultStrategy.class.getName(), ReplaceOneDefaultStrategy.class);
            put(UpdateOneTimestampsStrategy.class.getName(), UpdateOneTimestampsStrategy.class);
        }};

        candidates.entrySet().forEach(entry -> {
            HashMap<String, String> map = new HashMap<>();
            if (!entry.getKey().isEmpty()) {
                map.put(MONGODB_WRITEMODEL_STRATEGY, entry.getKey());
            }
            MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
            WriteModelStrategy wms = cfg.getWriteModelStrategy("");
            tests.add(dynamicTest(entry.getKey().isEmpty() ? "check write model strategy for default config" :
                            "check write model strategy for config " + MONGODB_WRITEMODEL_STRATEGY + "=" + entry.getKey(),
                    () -> assertAll("check for non-null and correct type",
                            () -> assertNotNull(wms, "write model strategy was null"),
                            () -> assertTrue(entry.getValue().isInstance(wms),
                                    "write model strategy NOT of type " + entry.getValue().getName())
                    )
            ));
        });

        return tests;
    }

    @TestFactory
    @DisplayName("test get multiple collection specific valid write model strategies")
    public Collection<DynamicTest> testGetMultipleCollectionSpecificValidWriteModelStrategy() {

        List<DynamicTest> tests = new ArrayList<>();

        Map<String, Class> canditates = new HashMap<String, Class>() {{
            put("", ReplaceOneDefaultStrategy.class);
            put("collection-1", ReplaceOneDefaultStrategy.class);
            put("collection-2", ReplaceOneBusinessKeyStrategy.class);
            put("collection-3", UpdateOneTimestampsStrategy.class);
            put("collection-4", DeleteOneDefaultStrategy.class);
        }};

        HashMap<String, String> map = new HashMap<>();
        canditates.entrySet().forEach(entry ->
                map.put(MONGODB_WRITEMODEL_STRATEGY + "." + entry.getKey(), entry.getValue().getName())
        );
        map.put(MONGODB_COLLECTIONS_CONF, canditates.keySet().stream()
                .collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)));

        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
        Map<String, WriteModelStrategy> wms = cfg.getWriteModelStrategies();

        wms.entrySet().forEach(entry ->
                tests.add(dynamicTest(
                        TOPIC_AGNOSTIC_KEY_NAME.equals(entry.getKey())
                                ? "check fallback write model strategy for config " + MONGODB_WRITEMODEL_STRATEGY
                                : "check write model strategy for config " + MONGODB_WRITEMODEL_STRATEGY + "." + entry.getKey(),
                        () -> assertAll("check for non-null and correct type",
                                () -> assertNotNull(entry.getValue(), "write model strategy was null"),
                                () -> assertTrue(canditates.get(TOPIC_AGNOSTIC_KEY_NAME.equals(entry.getKey())
                                                ? "" : entry.getKey()).isInstance(entry.getValue()),
                                        "write model strategy NOT of type " + canditates.get(entry.getKey()))
                        ))
                )
        );

        return tests;
    }

    @TestFactory
    @DisplayName("test get single valid cdc handler")
    public Collection<DynamicTest> testGetSingleValidCdcHandler() {

        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, Class> candidates = new HashMap<String, Class>() {{
            put(MongoDbHandler.class.getName(), MongoDbHandler.class);
            put(MysqlHandler.class.getName(), MysqlHandler.class);
            put(PostgresHandler.class.getName(), PostgresHandler.class);
        }};

        candidates.entrySet().forEach(entry -> {
            HashMap<String, String> map = new HashMap<>();
            map.put(MONGODB_CHANGE_DATA_CAPTURE_HANDLER, entry.getKey());
            MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
            CdcHandler cdc = cfg.getCdcHandler("");
            tests.add(dynamicTest("check cdc handler for config"
                            + MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "=" + entry.getKey(),
                    () -> assertAll("check for non-null and correct type",
                            () -> assertNotNull(cdc, "cdc handler was null"),
                            () -> assertTrue(entry.getValue().isInstance(cdc),
                                    "cdc handler NOT of type " + entry.getValue().getName())
                    )
            ));
        });

        tests.add(dynamicTest("check cdc handler for config"
                        + MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "=",
                () -> assertNull(new MongoDbSinkConnectorConfig(new HashMap<>()).getCdcHandler(""),
                        "cdc handler was not null")
                )
        );

        return tests;
    }

    @TestFactory
    @DisplayName("test get multiple collection specific valid cdc handlers")
    public Collection<DynamicTest> testGetMultipleCollectionSpecificValidCdcHandlers() {

        List<DynamicTest> tests = new ArrayList<>();

        Map<String, Class> canditates = new HashMap<String, Class>() {{
            put("collection-1", MongoDbHandler.class);
            put("collection-2", MysqlHandler.class);
            put("collection-3", PostgresHandler.class);
        }};

        HashMap<String, String> map = new HashMap<>();
        canditates.entrySet().forEach(entry ->
                map.put(MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "." + entry.getKey(), entry.getValue().getName())
        );
        map.put(MONGODB_COLLECTIONS_CONF, canditates.keySet().stream()
                .collect(Collectors.joining(FIELD_LIST_SPLIT_CHAR)));

        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);
        Map<String, CdcHandler> cdc = cfg.getCdcHandlers();

        cdc.entrySet().forEach(entry ->
                tests.add(dynamicTest("check cdc handler for config " +
                                MONGODB_CHANGE_DATA_CAPTURE_HANDLER + "." + entry.getKey(),
                        () -> assertAll("check for non-null and correct type",
                                () -> assertNotNull(entry.getValue(), "cdc handler was null"),
                                () -> assertTrue(canditates.get(entry.getKey()).isInstance(entry.getValue()),
                                        "cdc handler NOT of type " + canditates.get(entry.getKey()))
                        ))
                )
        );

        return tests;
    }

}
