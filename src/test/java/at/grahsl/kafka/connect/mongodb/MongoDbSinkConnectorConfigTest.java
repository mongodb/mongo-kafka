package at.grahsl.kafka.connect.mongodb;

import com.github.jcustenborder.kafka.connect.utils.config.MarkdownFormatter;
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

import java.util.*;

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

        Map<String, String> map = new HashMap<String,String>() {{
            put(MongoDbSinkConnectorConfig.MONGODB_AUTH_ACTIVE_CONF, "true");
            put(MongoDbSinkConnectorConfig.MONGODB_USERNAME_CONF, "hanspeter");
            put(MongoDbSinkConnectorConfig.MONGODB_PASSWORD_CONF, "secret");
        }};

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(map);

        MongoClientURI uri = cfg.buildClientURI();

        assertEquals(CLIENT_URI_AUTH_SETTINGS, uri.toString(), "wrong connection uri");

    }

    @Test
    @DisplayName("test K/V projection list with invalid projection type")
    public void getProjectionListsForInvalidProjectionTypes() {

        assertAll("try invalid projection types for key and value list",
                () -> assertThrows(ConfigException.class, () ->
                        new MongoDbSinkConnectorConfig(
                                new HashMap<String, String>() {{
                                    put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "invalid");
                                }}
                        )),
                () -> assertThrows(ConfigException.class, () ->
                        new MongoDbSinkConnectorConfig(
                                new HashMap<String, String>() {{
                                    put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "invalid");
                                }}
                        ))
        );

    }

    @Test
    @DisplayName("test empty K/V projection field list when type 'none'")
    public void getEmptyKeyValueProjectionFieldListsForNoneType() {

        MongoDbSinkConnectorConfig cfgKeyTypeNone =
                new MongoDbSinkConnectorConfig(
                        new HashMap<String, String>() {{
                            put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "none");
                            put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF, "useless,because,ignored");
                        }}
                );

        MongoDbSinkConnectorConfig cfgValueTypeNone =
                new MongoDbSinkConnectorConfig(
                        new HashMap<String, String>() {{
                            put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "none");
                            put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF, "useless,because,ignored");
                        }}
                );

        assertAll("test for empty field sets when type is none",
                () -> assertThat(cfgKeyTypeNone.getKeyProjectionList(), CoreMatchers.is(Matchers.empty())),
                () -> assertThat(cfgValueTypeNone.getKeyProjectionList(), CoreMatchers.is(Matchers.empty()))
        );

    }

    @Test
    @DisplayName("test correct field set for K/V projection when type is 'blacklist'")
    public void getCorrectFieldSetForKeyAndValueBlacklistProjectionList() {

        String fieldList = "field1,field2.subA,field2.subB,field3.**";

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(
                        new HashMap<String, String>() {{
                            put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "blacklist");
                            put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF,
                                    fieldList);
                            put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "blacklist");
                            put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF,
                                    fieldList);
                        }}
                );

        Set<String> blacklisted = new HashSet<String>() {{
                addAll(Arrays.asList("field1","field2.subA","field2.subB","field3.**"));
        }};

        assertAll("test correct field set for K/V blacklist projection",
                () -> assertThat(cfg.getKeyProjectionList(), Matchers.containsInAnyOrder(blacklisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(), Matchers.containsInAnyOrder(blacklisted.toArray()))
        );

    }

    @Test
    @DisplayName("test correct field set for K/V projection when type is 'whitelist'")
    public void getCorrectFieldSetForKeyAndValueWhiteListProjectionList() {

        String fieldList = "field1.**,field2.*.subSubA,field2.subB.*,field3.subC.subSubD";

        MongoDbSinkConnectorConfig cfg =
                new MongoDbSinkConnectorConfig(
                        new HashMap<String, String>() {{
                            put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "whitelist");
                            put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_LIST_CONF,
                                    fieldList);
                            put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_TYPE_CONF, "whitelist");
                            put(MongoDbSinkConnectorConfig.MONGODB_VALUE_PROJECTION_LIST_CONF,
                                    fieldList);
                        }}
                );

        //this test for all entries after doing left prefix expansion which is used for whitelisting
        Set<String> whitelisted = new HashSet<String>() {{
            addAll(Arrays.asList("field1","field1.**",
                                    "field2","field2.*","field2.*.subSubA",
                                    "field2.subB","field2.subB.*",
                                    "field3","field3.subC","field3.subC.subSubD"));
        }};

        assertAll("test correct field set for K/V whitelist projection",
                () -> assertThat(cfg.getKeyProjectionList(), Matchers.containsInAnyOrder(whitelisted.toArray())),
                () -> assertThat(cfg.getValueProjectionList(), Matchers.containsInAnyOrder(whitelisted.toArray()))
        );

    }

    @TestFactory
    @DisplayName("test get (in)valid id strategies")
    public Collection<DynamicTest> getIdStrategy() {

        List<DynamicTest> modeTests = new ArrayList<>();

        for(MongoDbSinkConnectorConfig.IdStrategyModes mode
                : MongoDbSinkConnectorConfig.IdStrategyModes.values()) {

            MongoDbSinkConnectorConfig cfgBL =
                    new MongoDbSinkConnectorConfig(
                            new HashMap<String, String>() {{
                                put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "blacklist");
                                put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF, mode.name());
                            }}
                    );

            modeTests.add(dynamicTest("blacklist: test id strategy for "+mode.name(),
                    () -> assertThat(cfgBL.getIdStrategy().getMode().name(),CoreMatchers.equalTo(mode.name()))
            ));

            MongoDbSinkConnectorConfig cfgWL =
                    new MongoDbSinkConnectorConfig(
                            new HashMap<String, String>() {{
                                put(MongoDbSinkConnectorConfig.MONGODB_KEY_PROJECTION_TYPE_CONF, "whitelist");
                                put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF, mode.name());
                            }}
                    );

            modeTests.add(dynamicTest("whitelist: test id strategy for "+mode.name(),
                    () -> assertThat(cfgWL.getIdStrategy().getMode().name(),CoreMatchers.equalTo(mode.name()))
            ));
        }

        String unknownStrategy = "INVALID";

        modeTests.add(dynamicTest("test id strategy for "+unknownStrategy,
                () -> assertThrows(ConfigException.class,
                        () -> new MongoDbSinkConnectorConfig(
                            new HashMap<String, String>() {{
                                put(MongoDbSinkConnectorConfig.MONGODB_DOCUMENT_ID_STRATEGY_CONF, unknownStrategy);
                        }}
                    )
                )
        ));

        return modeTests;

    }

}
