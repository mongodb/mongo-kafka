package at.grahsl.kafka.connect.mongodb;

import at.grahsl.kafka.connect.mongodb.processor.BlacklistValueProjector;
import at.grahsl.kafka.connect.mongodb.processor.DocumentIdAdder;
import at.grahsl.kafka.connect.mongodb.processor.PostProcessor;
import at.grahsl.kafka.connect.mongodb.processor.WhitelistValueProjector;
import at.grahsl.kafka.connect.mongodb.processor.field.renaming.RenameByMapping;
import at.grahsl.kafka.connect.mongodb.processor.field.renaming.RenameByRegExp;
import com.github.jcustenborder.kafka.connect.utils.config.MarkdownFormatter;
import com.mongodb.MongoClientURI;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.DynamicTest.*;

@RunWith(JUnitPlatform.class)
public class MongoDbSinkConnectorConfigTest {

    public static final String CLIENT_URI_DEFAULT_SETTINGS =
            "mongodb://localhost:27017/kafkaconnect?w=1";
    public static final String CLIENT_URI_AUTH_SETTINGS =
            "mongodb://hanspeter:secret@localhost:27017/kafkaconnect?authSource=admin&w=1";

    @Test
    @DisplayName("build config doc (no test)")
    public void doc() {
        System.out.println(MongoDbSinkConnectorConfig.conf().toRst());
        System.out.println(MarkdownFormatter.toMarkdown(MongoDbSinkConnectorConfig.conf()));
        assertTrue(true);
    }

    @Test
    @DisplayName("test client uri for default settings")
    public void buildClientUriWithDefaultSettings() {

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(new HashMap<>());

        MongoClientURI uri = cfg.buildClientURI();

        assertEquals(CLIENT_URI_DEFAULT_SETTINGS, uri.toString(), "wrong connection uri");

    }

    @Test
    @DisplayName("test client uri for configured auth settings")
    public void buildClientUriWithAuthSettings() {

        Map<String, String> map = new HashMap<String, String>();
        map.put(MongoDbSinkConnectorConfig.MONGODB_AUTH_ACTIVE_CONF, "true");
        map.put(MongoDbSinkConnectorConfig.MONGODB_USERNAME_CONF, "hanspeter");
        map.put(MongoDbSinkConnectorConfig.MONGODB_PASSWORD_CONF, "secret");

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(map);

        MongoClientURI uri = cfg.buildClientURI();

        assertEquals(CLIENT_URI_AUTH_SETTINGS, uri.toString(), "wrong connection uri");

    }

    @Test
    @DisplayName("test K/V projection list with invalid projection type")
    public void getProjectionListsForInvalidProjectionTypes() {

        assertAll("try invalid projection types for key and value list",
                () -> assertThrows(ConfigException.class, () -> {
                    HashMap<String, String> map = new HashMap<>();
                    map.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "invalid");
                    new MongoDbSinkConnectorConfig(map);
                }),
                () -> assertThrows(ConfigException.class, () -> {
                    HashMap<String, String> map = new HashMap<>();
                    map.put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "invalid");
                    new MongoDbSinkConnectorConfig(map);
                })
        );

    }

    @Test
    @DisplayName("test empty K/V projection field list when type 'none'")
    public void getEmptyKeyValueProjectionFieldListsForNoneType() {
        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "none");
        map1.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF, "useless,because,ignored");
        MongoDbSinkConnectorConfig cfgKeyTypeNone = new MongoDbSinkConnectorConfig(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "none");
        map2.put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF, "useless,because,ignored");
        MongoDbSinkConnectorConfig cfgValueTypeNone = new MongoDbSinkConnectorConfig(map2);

        assertAll("test for empty field sets when type is none",
                () -> assertThat(cfgKeyTypeNone.getKeyProjectionList(), CoreMatchers.is(Matchers.empty())),
                () -> assertThat(cfgValueTypeNone.getKeyProjectionList(), CoreMatchers.is(Matchers.empty()))
        );
    }

    @Test
    @DisplayName("test correct field set for K/V projection when type is 'blacklist'")
    public void getCorrectFieldSetForKeyAndValueBlacklistProjectionList() {
        String fieldList = "field1,field2.subA,field2.subB,field3.**";
        HashMap<String, String> map = new HashMap<>();
        map.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "blacklist");
        map.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF,
                fieldList);
        map.put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "blacklist");
        map.put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF,
                fieldList);
        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);

        Set<String> blacklisted = new HashSet<>();
        blacklisted.addAll(Arrays.asList("field1", "field2.subA", "field2.subB", "field3.**"));

        assertAll("test correct field set for K/V blacklist projection",
                () -> assertThat(cfg.getKeyProjectionList(), Matchers.containsInAnyOrder(blacklisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(), Matchers.containsInAnyOrder(blacklisted.toArray()))
        );
    }

    @Test
    @DisplayName("test correct field set for K/V projection when type is 'whitelist'")
    public void getCorrectFieldSetForKeyAndValueWhiteListProjectionList() {
        String fieldList = "field1.**,field2.*.subSubA,field2.subB.*,field3.subC.subSubD";
        HashMap<String, String> map = new HashMap<>();
        map.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "whitelist");
        map.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF,
                fieldList);
        map.put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "whitelist");
        map.put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF,
                fieldList);
        MongoDbSinkConnectorConfig cfg = new MongoDbSinkConnectorConfig(map);

        //this test for all entries after doing left prefix expansion which is used for whitelisting
        Set<String> whitelisted = new HashSet<String>();
        whitelisted.addAll(Arrays.asList("field1", "field1.**",
                "field2", "field2.*", "field2.*.subSubA",
                "field2.subB", "field2.subB.*",
                "field3", "field3.subC", "field3.subC.subSubD"));

        assertAll("test correct field set for K/V whitelist projection",
                () -> assertThat(cfg.getKeyProjectionList(), Matchers.containsInAnyOrder(whitelisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(), Matchers.containsInAnyOrder(whitelisted.toArray()))
        );

    }

    @TestFactory
    @DisplayName("test get (in)valid id strategies")
    public Collection<DynamicTest> getIdStrategy() {

        List<DynamicTest> modeTests = new ArrayList<>();

        for (MongoDbSinkConnectorConfig.IdStrategyModes mode
                : MongoDbSinkConnectorConfig.IdStrategyModes.values()) {

            HashMap<String, String> map1 = new HashMap<>();
            map1.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "blacklist");
            map1.put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF, mode.name());
            MongoDbSinkConnectorConfig cfgBL = new MongoDbSinkConnectorConfig(map1);

            modeTests.add(dynamicTest("blacklist: test id strategy for " + mode.name(),
                    () -> assertThat(cfgBL.getIdStrategy().getMode().name(), CoreMatchers.equalTo(mode.name()))
            ));

            HashMap<String, String> map2 = new HashMap<>();
            map2.put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "whitelist");
            map2.put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF, mode.name());
            MongoDbSinkConnectorConfig cfgWL = new MongoDbSinkConnectorConfig(map2);

            modeTests.add(dynamicTest("whitelist: test id strategy for " + mode.name(),
                    () -> assertThat(cfgWL.getIdStrategy().getMode().name(), CoreMatchers.equalTo(mode.name()))
            ));
        }

        String unknownStrategy = "INVALID";
        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF, unknownStrategy);
        modeTests.add(dynamicTest("test id strategy for " + unknownStrategy,
                () -> assertThrows(ConfigException.class, () -> new MongoDbSinkConnectorConfig(map3))
        ));

        return modeTests;

    }

    @TestFactory
    @DisplayName("test parse json rename field name mappings")
    public Collection<DynamicTest> getParseJsonRenameFieldnameMappings() {

        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING, "[]");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        tests.add(dynamicTest("parsing fieldname mapping for (" +
                        cfg1.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertEquals(new HashMap<>(), cfg1.parseRenameFieldnameMappings()))
        );

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING,
                "[{\"oldName\":\"key.fieldA\",\"newName\":\"field1\"},{\"oldName\":\"value.xyz\",\"newName\":\"abc\"}]");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        HashMap<String, String> result2 = new HashMap<>();
        result2.put("key.fieldA", "field1");
        result2.put("value.xyz", "abc");

        tests.add(dynamicTest("parsing fieldname mapping for (" +
                        cfg2.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertEquals(result2, cfg2.parseRenameFieldnameMappings()))
        );

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING, "]some {invalid JSON]}[");
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);

        tests.add(dynamicTest("parsing fieldname mapping for (" +
                        cfg3.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_MAPPING) + ")",
                () -> assertThrows(ConfigException.class, () -> cfg3.parseRenameFieldnameMappings()))
        );

        return tests;

    }

    @TestFactory
    @DisplayName("test parse json rename regexp settings")
    public Collection<DynamicTest> getParseJsonRenameRegExpSettings() {

        List<DynamicTest> tests = new ArrayList<>();

        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP, "[]");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        tests.add(dynamicTest("parsing regexp settings for (" +
                        cfg1.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertEquals(new HashMap<>(), cfg1.parseRenameRegExpSettings()))
        );

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP,
                "[{\"regexp\":\"^key\\\\..*my.*$\",\"pattern\":\"my\",\"replace\":\"\"}," +
                        "{\"regexp\":\"^value\\\\..*$\",\"pattern\":\"\\\\.\",\"replace\":\"_\"}]");

        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        HashMap<String, RenameByRegExp.PatternReplace> result2 = new HashMap<>();
        result2.put("^key\\..*my.*$", new RenameByRegExp.PatternReplace("my", ""));
        result2.put("^value\\..*$", new RenameByRegExp.PatternReplace("\\.", "_"));

        tests.add(dynamicTest("parsing regexp settings for (" +
                        cfg2.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertEquals(result2, cfg2.parseRenameRegExpSettings()))
        );

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP, "]some {invalid JSON]}[");
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);

        tests.add(dynamicTest("parsing regexp settings for (" +
                        cfg3.getString(MongoDbSinkConnectorConfig.MONGODB_FIELD_RENAMER_REGEXP) + ")",
                () -> assertThrows(ConfigException.class, () -> cfg3.parseRenameRegExpSettings()))
        );

        return tests;

    }

    @TestFactory
    @DisplayName("test build post processor chain for valid settings")
    public Collection<DynamicTest> getBuildPostProcessorChainValid() {

        List<DynamicTest> tests = new ArrayList<>();

        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(new HashMap<>());
        PostProcessor chain1 = cfg1.buildPostProcessorChain();
        tests.add(dynamicTest("check chain result if not specified and using default", () ->
                assertAll("check for type and no successor",
                        () -> assertTrue(chain1 instanceof DocumentIdAdder,
                                "post processor not of type " + DocumentIdAdder.class.getName()),
                        () -> assertEquals(Optional.empty(), chain1.getNext())
                )
        ));

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MongoDbSinkConnectorConfig.MONGODB_POST_PROCESSOR_CHAIN, "");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        PostProcessor chain2 = cfg2.buildPostProcessorChain();
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
                .collect(Collectors.joining(MongoDbSinkConnectorConfig.FIELD_LIST_SPLIT_CHAR));

        HashMap<String, String> map3 = new HashMap<>();
        map3.put(MongoDbSinkConnectorConfig.MONGODB_POST_PROCESSOR_CHAIN, processors);
        MongoDbSinkConnectorConfig cfg3 = new MongoDbSinkConnectorConfig(map3);
        PostProcessor chain3 = cfg3.buildPostProcessorChain();

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
                .collect(Collectors.joining(MongoDbSinkConnectorConfig.FIELD_LIST_SPLIT_CHAR));

        classes.add(0, docIdAdder);

        HashMap<String, String> map4 = new HashMap<>();
        map4.put(MongoDbSinkConnectorConfig.MONGODB_POST_PROCESSOR_CHAIN, processors);
        MongoDbSinkConnectorConfig cfg4 = new MongoDbSinkConnectorConfig(map4);
        PostProcessor chain4 = cfg4.buildPostProcessorChain();

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
    @DisplayName("test build post processor chain for invalid settings")
    public void getBuildPostProcessorChainInvalid() {
        HashMap<String, String> map1 = new HashMap<>();
        map1.put(MongoDbSinkConnectorConfig.MONGODB_POST_PROCESSOR_CHAIN,
                "at.grahsl.class.NotExisting");
        MongoDbSinkConnectorConfig cfg1 = new MongoDbSinkConnectorConfig(map1);

        HashMap<String, String> map2 = new HashMap<>();
        map2.put(MongoDbSinkConnectorConfig.MONGODB_POST_PROCESSOR_CHAIN,
                "at.grahsl.kafka.connect.mongodb.MongoDbSinkTask");
        MongoDbSinkConnectorConfig cfg2 = new MongoDbSinkConnectorConfig(map2);

        assertAll("check for not existing and invalid class used for post processor",
                () -> assertThrows(ConfigException.class, () -> cfg1.buildPostProcessorChain()),
                () -> assertThrows(ConfigException.class, () -> cfg2.buildPostProcessorChain())
        );
    }
}
