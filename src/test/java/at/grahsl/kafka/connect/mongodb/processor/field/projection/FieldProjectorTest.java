package at.grahsl.kafka.connect.mongodb.processor.field.projection;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;
import at.grahsl.kafka.connect.mongodb.converter.SinkDocument;
import at.grahsl.kafka.connect.mongodb.processor.BlacklistKeyProjector;
import at.grahsl.kafka.connect.mongodb.processor.BlacklistValueProjector;
import at.grahsl.kafka.connect.mongodb.processor.WhitelistKeyProjector;
import at.grahsl.kafka.connect.mongodb.processor.WhitelistValueProjector;
import org.apache.kafka.connect.errors.DataException;
import org.bson.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnitPlatform.class)
public class FieldProjectorTest {

    //flat doc field maps
    static Map<Set<String>,BsonDocument> flatKeyFieldsMapBlacklist = new HashMap<>();
    static Map<Set<String>,BsonDocument> flatKeyFieldsMapWhitelist = new HashMap<>();

    static Map<Set<String>,BsonDocument> flatValueFieldsMapBlacklist = new HashMap<>();
    static Map<Set<String>,BsonDocument> flatValueFieldsMapWhitelist = new HashMap<>();

    //nested doc field maps
    static Map<Set<String>,BsonDocument> nestedKeyFieldsMapBlacklist = new HashMap<>();
    static Map<Set<String>,BsonDocument> nestedKeyFieldsMapWhitelist = new HashMap<>();

    static Map<Set<String>,BsonDocument> nestedValueFieldsMapBlacklist = new HashMap<>();
    static Map<Set<String>,BsonDocument> nestedValueFieldsMapWhitelist = new HashMap<>();

    @BeforeAll
    static void setupFlatDocMaps() {

        // NOTE: FieldProjectors are currently implemented so that
        // a) when blacklisting: already present _id fields are never removed even if specified
        // and
        // b) when whitelisting: already present _id fields are always kept even if not specified

        //key projection settings
        flatKeyFieldsMapBlacklist.put(new HashSet<>(),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myBoolean",new BsonBoolean(true));
                    put("myInt",new BsonInt32(42));
                    put("myBytes",new BsonBinary(new byte[] {65,66,67}));
                    put("myArray", new BsonArray());
                }}
        );

        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("*")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                }}
        );

        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                }}
        );

        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("_id")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myBoolean",new BsonBoolean(true));
                    put("myInt",new BsonInt32(42));
                    put("myBytes",new BsonBinary(new byte[] {65,66,67}));
                    put("myArray", new BsonArray());
                }}
        );

        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("myBoolean","myInt")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myBytes",new BsonBinary(new byte[] {65,66,67}));
                    put("myArray", new BsonArray());
                }}
       );

        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myBoolean",new BsonBoolean(true));
                    put("myInt",new BsonInt32(42));
                    put("myBytes",new BsonBinary(new byte[] {65,66,67}));
                    put("myArray", new BsonArray());
                }}
        );

        flatKeyFieldsMapWhitelist.put(new HashSet<>(),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                }}
        );

        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("*")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myBoolean",new BsonBoolean(true));
                    put("myInt",new BsonInt32(42));
                    put("myBytes",new BsonBinary(new byte[] {65,66,67}));
                    put("myArray", new BsonArray());
                }}
        );

        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myBoolean",new BsonBoolean(true));
                    put("myInt",new BsonInt32(42));
                    put("myBytes",new BsonBinary(new byte[] {65,66,67}));
                    put("myArray", new BsonArray());
                }}
        );

        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                }}
        );

        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("myBoolean","myBytes","myArray")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myBoolean",new BsonBoolean(true));
                    put("myBytes",new BsonBinary(new byte[] {65,66,67}));
                    put("myArray", new BsonArray());
                }}
        );

        //value projection settings
        flatValueFieldsMapBlacklist.put(new HashSet<>(),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myLong",new BsonInt64(42L));
                    put("myDouble",new BsonDouble(23.23d));
                    put("myString",new BsonString("BSON"));
                    put("myBytes",new BsonBinary(new byte[] {120,121,122}));
                    put("myArray", new BsonArray());
                }}
        );

        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("*")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                }}
        );

        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                }});

        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("_id")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myLong",new BsonInt64(42L));
                    put("myDouble",new BsonDouble(23.23d));
                    put("myString",new BsonString("BSON"));
                    put("myBytes",new BsonBinary(new byte[] {120,121,122}));
                    put("myArray", new BsonArray());
                }}
        );

        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("myLong","myDouble")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myString",new BsonString("BSON"));
                    put("myBytes",new BsonBinary(new byte[] {120,121,122}));
                    put("myArray", new BsonArray());
                }}
        );

        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myLong",new BsonInt64(42L));
                    put("myDouble",new BsonDouble(23.23d));
                    put("myString",new BsonString("BSON"));
                    put("myBytes",new BsonBinary(new byte[] {120,121,122}));
                    put("myArray", new BsonArray());
                }}
        );

        flatValueFieldsMapWhitelist.put(new HashSet<>(),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                }}
        );

        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("*")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myLong",new BsonInt64(42L));
                    put("myDouble",new BsonDouble(23.23d));
                    put("myString",new BsonString("BSON"));
                    put("myBytes",new BsonBinary(new byte[] {120,121,122}));
                    put("myArray", new BsonArray());
                }}
        );

        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myLong",new BsonInt64(42L));
                    put("myDouble",new BsonDouble(23.23d));
                    put("myString",new BsonString("BSON"));
                    put("myBytes",new BsonBinary(new byte[] {120,121,122}));
                    put("myArray", new BsonArray());
                }}
        );

        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                }}
        );

        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("myDouble","myBytes","myArray")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myDouble",new BsonDouble(23.23d));
                    put("myBytes",new BsonBinary(new byte[] {120,121,122}));
                    put("myArray", new BsonArray());
                }}
        );

    }

    @BeforeAll
    static void setupNestedFieldLists() {

        // NOTE: FieldProjectors are currently implemented so that
        // a) when blacklisting: already present _id fields are never removed even if specified
        // and
        // b) when whitelisting: already present _id fields are always kept even if not specified

        nestedKeyFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("_id", "subDoc1.myString", "subDoc2.myBoolean")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myInt", new BsonInt32(42));
                    put("subDoc1", new BsonDocument() {{
                        put("myBoolean", new BsonBoolean(false));
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myString", new BsonString("BSON2"));
                    }});
                }}
        );

        nestedKeyFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("*")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("subDoc1", new BsonDocument() {{
                        put("myString", new BsonString("BSON1"));
                        put("myBoolean", new BsonBoolean(false));
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myString", new BsonString("BSON2"));
                        put("myBoolean", new BsonBoolean(true));
                    }});
                }}
        );

        nestedKeyFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                }}
        );

        nestedKeyFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("*.myString")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("subDoc1", new BsonDocument() {{
                        put("myBoolean", new BsonBoolean(false));
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myBoolean", new BsonBoolean(true));
                    }});
                }}
        );

        nestedKeyFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("*.*")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("subDoc1", new BsonDocument());
                    put("subDoc2", new BsonDocument());
                }}
        );

        nestedKeyFieldsMapWhitelist.put(new HashSet<>(),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                }}
        );

        nestedKeyFieldsMapWhitelist.put(new HashSet<>(
                Arrays.asList("*")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myInt", new BsonInt32(42));
                    put("subDoc1", new BsonDocument());
                    put("subDoc2", new BsonDocument());
                }}
        );

        nestedKeyFieldsMapWhitelist.put(new HashSet<>(
                        Arrays.asList("*","*.myBoolean")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myInt", new BsonInt32(42));
                    put("subDoc1", new BsonDocument() {{
                        put("myBoolean", new BsonBoolean(false));
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myBoolean", new BsonBoolean(true));
                    }});
                }}
        );

        nestedKeyFieldsMapWhitelist.put(new HashSet<>(
                        Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("ABC-123"));
                    put("myInt", new BsonInt32(42));
                    put("subDoc1", new BsonDocument() {{
                        put("myString", new BsonString("BSON1"));
                        put("myBoolean", new BsonBoolean(false));
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myString", new BsonString("BSON2"));
                        put("myBoolean", new BsonBoolean(true));
                    }});
                }}
        );

        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                Arrays.asList("_id","subDoc1","subDoc2")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myBoolean", new BsonBoolean(true));
                }}
        );

        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                }}
        );

        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("subDoc1.subSubDoc","subDoc2")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myBoolean", new BsonBoolean(true));
                    put("subDoc1", new BsonDocument() {{
                        put("myFieldA", new BsonString("some text"));
                        put("myFieldB", new BsonDouble(12.34d));
                    }});
                }}
        );

        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("*","subDoc1.*","subDoc2.**")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("subDoc1", new BsonDocument() {{
                        put("subSubDoc", new BsonDocument() {{
                            put("myString", new BsonString("some text"));
                            put("myInt", new BsonInt32(0));
                            put("myBoolean", new BsonBoolean(false));
                        }});
                    }});
                    put("subDoc2", new BsonDocument());
                }}
        );

        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("*.*")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("subDoc1", new BsonDocument() {{
                        put("subSubDoc", new BsonDocument() {{
                            put("myString", new BsonString("some text"));
                            put("myInt", new BsonInt32(0));
                            put("myBoolean", new BsonBoolean(false));
                        }});
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("subSubDoc", new BsonDocument() {{
                            put("myBytes", new BsonBinary(new byte[]{120, 121, 122}));
                            put("myArray", new BsonArray());
                        }});
                    }});
                }}
        );

        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("*.subSubDoc")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("subDoc1", new BsonDocument() {{
                        put("myFieldA", new BsonString("some text"));
                        put("myFieldB", new BsonDouble(12.34d));
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myFieldA", new BsonString("some text"));
                        put("myFieldB", new BsonDouble(12.34d));
                    }});
                }}
        );


        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("subDoc1.*.myString","subDoc2.subSubDoc.*")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myBoolean", new BsonBoolean(true));
                    put("subDoc1", new BsonDocument() {{
                        put("subSubDoc", new BsonDocument() {{
                            put("myInt", new BsonInt32(0));
                            put("myBoolean", new BsonBoolean(false));
                        }});
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myFieldA", new BsonString("some text"));
                        put("myFieldB", new BsonDouble(12.34d));
                        put("subSubDoc", new BsonDocument());
                    }});
                }}
        );

        nestedValueFieldsMapWhitelist.put(new HashSet<>(),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                }}
        );

        nestedValueFieldsMapWhitelist.put(new HashSet<>(
                Arrays.asList("*")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myBoolean", new BsonBoolean(true));
                    put("subDoc1", new BsonDocument());
                    put("subDoc2", new BsonDocument());
                }}
        );

        nestedValueFieldsMapWhitelist.put(new HashSet<>(
                Arrays.asList("subDoc1","subDoc1.**",
                                "subDoc2","subDoc2.subSubDoc","subDoc2.subSubDoc.myArray")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("subDoc1", new BsonDocument() {{
                        put("myFieldA", new BsonString("some text"));
                        put("myFieldB", new BsonDouble(12.34d));
                        put("subSubDoc", new BsonDocument() {{
                            put("myString", new BsonString("some text"));
                            put("myInt", new BsonInt32(0));
                            put("myBoolean", new BsonBoolean(false));
                        }});
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("subSubDoc", new BsonDocument() {{
                            put("myArray", new BsonArray());
                        }});
                    }});
                }}
        );

        nestedValueFieldsMapWhitelist.put(new HashSet<>(
                        Arrays.asList("**")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myBoolean", new BsonBoolean(true));
                    put("subDoc1", new BsonDocument() {{
                        put("myFieldA", new BsonString("some text"));
                        put("myFieldB", new BsonDouble(12.34d));
                        put("subSubDoc", new BsonDocument() {{
                            put("myString", new BsonString("some text"));
                            put("myInt", new BsonInt32(0));
                            put("myBoolean", new BsonBoolean(false));
                        }});
                    }});
                    put("subDoc2", new BsonDocument() {{
                        put("myFieldA", new BsonString("some text"));
                        put("myFieldB", new BsonDouble(12.34d));
                        put("subSubDoc", new BsonDocument() {{
                            put("myBytes", new BsonBinary(new byte[]{120, 121, 122}));
                            put("myArray", new BsonArray());
                        }});
                    }});
                }}
        );

    }

    @TestFactory
    @DisplayName("testing different projector settings for flat structure")
    List<DynamicTest> testProjectorSettingsOnFlatStructure() {

        List<DynamicTest> tests = new ArrayList<>();

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatKeyFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistKeyProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, BlacklistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatKeyFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistKeyProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, WhitelistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatValueFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistValueProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, BlacklistValueProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatValueFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistValueProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, WhitelistValueProjector.class, cfg));

        }

        return tests;
    }

    @TestFactory
    @DisplayName("testing different projector settings for nested structure")
    List<DynamicTest> testProjectorSettingsOnNestedStructure() {

        List<DynamicTest> tests = new ArrayList<>();

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedKeyFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistKeyProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentNestedStruct(),
                    entry, BlacklistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedKeyFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistKeyProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentNestedStruct(),
                    entry, WhitelistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedValueFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistValueProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentNestedStruct(),
                    entry, BlacklistValueProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedValueFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList()).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistValueProjection()).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentNestedStruct(),
                    entry, WhitelistValueProjector.class, cfg));

        }

        return tests;
    }

    private static DynamicTest buildDynamicTestFor(SinkDocument doc,
                                                   Map.Entry<Set<String>,BsonDocument> entry, Class<? extends FieldProjector> clazz,
                                                   MongoDbSinkConnectorConfig cfg) {

        return dynamicTest(clazz.getSimpleName() +" with "+entry.getKey().toString(), () -> {

            FieldProjector fp = (FieldProjector)Class.forName(clazz.getName())
                    .getConstructor(MongoDbSinkConnectorConfig.class).newInstance(cfg);

            fp.process(doc, null);

            assertEquals(entry.getValue(), extractBsonDocument(doc,fp));

        });

    }

    private static BsonDocument extractBsonDocument(SinkDocument doc,FieldProjector which) {

        if(which instanceof BlacklistKeyProjector
                || which instanceof WhitelistKeyProjector)

            return doc.getKeyDoc().orElseThrow(
                    () -> new DataException("the needed BSON key doc was not present"));


        if(which instanceof BlacklistValueProjector
                || which instanceof WhitelistValueProjector)

            return doc.getValueDoc().orElseThrow(
                    () -> new DataException("the needed BSON value was not present"));

        throw new IllegalArgumentException("unexpected projector type "+which.getClass().getName());

    }

    private static SinkDocument buildSinkDocumentFlatStruct() {

        BsonDocument flatKey = new BsonDocument() {{
            put("_id", new BsonString("ABC-123"));
            put("myBoolean",new BsonBoolean(true));
            put("myInt",new BsonInt32(42));
            put("myBytes",new BsonBinary(new byte[] {65,66,67}));
            put("myArray", new BsonArray());
        }};

        BsonDocument flatValue = new BsonDocument() {{
            put("_id", new BsonString("XYZ-789"));
            put("myLong",new BsonInt64(42L));
            put("myDouble",new BsonDouble(23.23d));
            put("myString",new BsonString("BSON"));
            put("myBytes",new BsonBinary(new byte[] {120,121,122}));
            put("myArray", new BsonArray());
        }};

        return new SinkDocument(flatKey,flatValue);
    }

    private static SinkDocument buildSinkDocumentNestedStruct() {

        BsonDocument nestedKey = new BsonDocument() {{
            put("_id", new BsonString("ABC-123"));
            put("myInt", new BsonInt32(42));
            put("subDoc1", new BsonDocument() {{
                put("myString", new BsonString("BSON1"));
                put("myBoolean", new BsonBoolean(false));
            }});
            put("subDoc2", new BsonDocument() {{
                put("myString", new BsonString("BSON2"));
                put("myBoolean", new BsonBoolean(true));
            }});
        }};

        BsonDocument nestedValue = new BsonDocument() {{
            put("_id", new BsonString("XYZ-789"));
            put("myBoolean", new BsonBoolean(true));
            put("subDoc1", new BsonDocument() {{
                put("myFieldA", new BsonString("some text"));
                put("myFieldB", new BsonDouble(12.34d));
                put("subSubDoc", new BsonDocument() {{
                    put("myString", new BsonString("some text"));
                    put("myInt", new BsonInt32(0));
                    put("myBoolean", new BsonBoolean(false));
                }});
            }});
            put("subDoc2", new BsonDocument() {{
                put("myFieldA", new BsonString("some text"));
                put("myFieldB", new BsonDouble(12.34d));
                put("subSubDoc", new BsonDocument() {{
                    put("myBytes", new BsonBinary(new byte[]{120, 121, 122}));
                    put("myArray", new BsonArray());
                }});
            }});
        }};

        return new SinkDocument(nestedKey,nestedValue);
    }

}
