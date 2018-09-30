/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    private static Map<Set<String>,BsonDocument> flatKeyFieldsMapBlacklist = new HashMap<>();
    private static Map<Set<String>,BsonDocument> flatKeyFieldsMapWhitelist = new HashMap<>();

    private static Map<Set<String>,BsonDocument> flatValueFieldsMapBlacklist = new HashMap<>();
    private static Map<Set<String>,BsonDocument> flatValueFieldsMapWhitelist = new HashMap<>();

    //nested doc field maps
    private static Map<Set<String>,BsonDocument> nestedKeyFieldsMapBlacklist = new HashMap<>();
    private static Map<Set<String>,BsonDocument> nestedKeyFieldsMapWhitelist = new HashMap<>();

    private static Map<Set<String>,BsonDocument> nestedValueFieldsMapBlacklist = new HashMap<>();
    private static Map<Set<String>,BsonDocument> nestedValueFieldsMapWhitelist = new HashMap<>();

    @BeforeAll
    public static void setupFlatDocMaps() {

        // NOTE: FieldProjectors are currently implemented so that
        // a) when blacklisting: already present _id fields are never removed even if specified
        // and
        // b) when whitelisting: already present _id fields are always kept even if not specified

        //key projection settings
        BsonDocument expectedDoc1 = new BsonDocument();
        expectedDoc1.put("_id", new BsonString("ABC-123"));
        expectedDoc1.put("myBoolean",new BsonBoolean(true));
        expectedDoc1.put("myInt",new BsonInt32(42));
        expectedDoc1.put("myBytes",new BsonBinary(new byte[] {65,66,67}));
        expectedDoc1.put("myArray", new BsonArray());
        flatKeyFieldsMapBlacklist.put(new HashSet<>(),expectedDoc1);

        BsonDocument expectedDoc2 = new BsonDocument();
        expectedDoc2.put("_id", new BsonString("ABC-123"));
        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("*")),
                expectedDoc2);

        BsonDocument expectedDoc3 = expectedDoc2;
        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("**")),
                expectedDoc3
        );

        BsonDocument expectedDoc4 = expectedDoc1;
        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("_id")),
                expectedDoc4
        );

        BsonDocument expectedDoc5 = new BsonDocument();
        expectedDoc5.put("_id", new BsonString("ABC-123"));
        expectedDoc5.put("myBytes",new BsonBinary(new byte[] {65,66,67}));
        expectedDoc5.put("myArray", new BsonArray());
        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("myBoolean","myInt")),
                expectedDoc5
        );

        BsonDocument expectedDoc6 = expectedDoc1;
        flatKeyFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                expectedDoc6
        );

        BsonDocument expectedDoc7 = expectedDoc2;
        flatKeyFieldsMapWhitelist.put(new HashSet<>(),
                expectedDoc7
        );

        BsonDocument expectedDoc8 = expectedDoc1;
        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("*")),
                expectedDoc8
        );

        BsonDocument expectedDoc9 = expectedDoc1;
        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("**")),
                expectedDoc9
        );

        BsonDocument expectedDoc10 = expectedDoc2;
        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                expectedDoc10
        );

        BsonDocument expectedDoc11 = new BsonDocument();
        expectedDoc11.put("_id", new BsonString("ABC-123"));
        expectedDoc11.put("myBoolean",new BsonBoolean(true));
        expectedDoc11.put("myBytes",new BsonBinary(new byte[] {65,66,67}));
        expectedDoc11.put("myArray", new BsonArray());
        flatKeyFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("myBoolean","myBytes","myArray")),
                expectedDoc11
        );

        //value projection settings
        BsonDocument expectedDoc12 = new BsonDocument();
        expectedDoc12.put("_id", new BsonString("XYZ-789"));
        expectedDoc12.put("myLong",new BsonInt64(42L));
        expectedDoc12.put("myDouble",new BsonDouble(23.23d));
        expectedDoc12.put("myString",new BsonString("BSON"));
        expectedDoc12.put("myBytes",new BsonBinary(new byte[] {120,121,122}));
        expectedDoc12.put("myArray", new BsonArray());
        flatValueFieldsMapBlacklist.put(new HashSet<>(),
                expectedDoc12
        );

        BsonDocument expectedDoc13 = new BsonDocument();
        expectedDoc13.put("_id", new BsonString("XYZ-789"));
        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("*")),
                expectedDoc13
        );

        BsonDocument expectedDoc14 = expectedDoc13;
        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("**")),
                expectedDoc14
        );

        BsonDocument expectedDoc15 = expectedDoc12;
        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("_id")),
                expectedDoc15
        );

        BsonDocument expectedDoc16 = new BsonDocument();
        expectedDoc16.put("_id", new BsonString("XYZ-789"));
        expectedDoc16.put("myString",new BsonString("BSON"));
        expectedDoc16.put("myBytes",new BsonBinary(new byte[] {120,121,122}));
        expectedDoc16.put("myArray", new BsonArray());
        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("myLong","myDouble")),
                expectedDoc16
        );

        BsonDocument expectedDoc17 = expectedDoc12;
        flatValueFieldsMapBlacklist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                expectedDoc17
        );

        BsonDocument expectedDoc18 = expectedDoc13;
        flatValueFieldsMapWhitelist.put(new HashSet<>(),
                expectedDoc18
        );

        BsonDocument expectedDoc19 = expectedDoc12;
        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("*")),
                expectedDoc19
        );

        BsonDocument expectedDoc20 = expectedDoc12;
        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("**")),
                expectedDoc20
        );

        BsonDocument expectedDoc21 = expectedDoc13;
        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("missing1","unknown2")),
                expectedDoc21
        );

        BsonDocument expectedDoc22 = new BsonDocument();
        expectedDoc22.put("_id", new BsonString("XYZ-789"));
        expectedDoc22.put("myDouble",new BsonDouble(23.23d));
        expectedDoc22.put("myBytes",new BsonBinary(new byte[] {120,121,122}));
        expectedDoc22.put("myArray", new BsonArray());
        flatValueFieldsMapWhitelist.put(new HashSet<>(Arrays.asList("myDouble","myBytes","myArray")),
                expectedDoc22
        );

    }

    @BeforeAll
    public static void setupNestedFieldLists() {

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
                            put("myArray", new BsonArray(
                                            Arrays.asList(
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("abc"));
                                                        put("value",new BsonInt32(123));
                                                    }},
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("xyz"));
                                                        put("value",new BsonInt32(987));
                                                    }}
                                            )
                                    )
                            );
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

        nestedValueFieldsMapBlacklist.put(new HashSet<>(
                        Arrays.asList("subDoc1","subDoc2.myFieldA","subDoc2.myFieldB",
                                        "subDoc2.subSubDoc.myBytes", "subDoc2.subSubDoc.myArray.key")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("myBoolean", new BsonBoolean(true));
                    put("subDoc2", new BsonDocument() {{
                        put("subSubDoc", new BsonDocument() {{
                            put("myArray", new BsonArray(
                                    Arrays.asList(
                                            new BsonDocument() {{
                                                put("value",new BsonInt32(123));
                                            }},
                                            new BsonDocument() {{
                                                put("value",new BsonInt32(987));
                                            }}
                                    )
                                    )
                            );
                        }});
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
                                "subDoc2","subDoc2.subSubDoc","subDoc2.subSubDoc.myArray",
                                    "subDoc2.subSubDoc.myArray.*")),
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
                            put("myArray", new BsonArray(
                                            Arrays.asList(
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("abc"));
                                                        put("value",new BsonInt32(123));
                                                    }},
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("xyz"));
                                                        put("value",new BsonInt32(987));
                                                    }}
                                            )
                                    )
                            );
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
                            put("myArray", new BsonArray(
                                            Arrays.asList(
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("abc"));
                                                        put("value",new BsonInt32(123));
                                                    }},
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("xyz"));
                                                        put("value",new BsonInt32(987));
                                                    }}
                                            )
                                    )
                            );
                        }});
                    }});
                }}
        );

        nestedValueFieldsMapWhitelist.put(new HashSet<>(
                        Arrays.asList("subDoc2","subDoc2.subSubDoc",
                                            "subDoc2.subSubDoc.myArray",
                                                "subDoc2.subSubDoc.myArray.key")),
                new BsonDocument() {{
                    put("_id", new BsonString("XYZ-789"));
                    put("subDoc2", new BsonDocument() {{
                        put("subSubDoc", new BsonDocument() {{
                            put("myArray", new BsonArray(
                                            Arrays.asList(
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("abc"));
                                                    }},
                                                    new BsonDocument() {{
                                                        put("key",new BsonString("xyz"));
                                                    }}
                                            )
                                    )
                            );
                        }});
                    }});
                }}
        );

    }

    @TestFactory
    @DisplayName("testing different projector settings for flat structure")
    public List<DynamicTest> testProjectorSettingsOnFlatStructure() {

        List<DynamicTest> tests = new ArrayList<>();

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatKeyFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistKeyProjection("")).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, BlacklistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatKeyFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistKeyProjection("")).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, WhitelistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatValueFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistValueProjection("")).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, BlacklistValueProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : flatValueFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistValueProjection("")).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentFlatStruct(),
                    entry, WhitelistValueProjector.class, cfg));

        }

        return tests;
    }

    @TestFactory
    @DisplayName("testing different projector settings for nested structure")
    public List<DynamicTest> testProjectorSettingsOnNestedStructure() {

        List<DynamicTest> tests = new ArrayList<>();

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedKeyFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistKeyProjection("")).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentNestedStruct(),
                    entry, BlacklistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedKeyFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getKeyProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistKeyProjection("")).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentNestedStruct(),
                    entry, WhitelistKeyProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedValueFieldsMapBlacklist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingBlacklistValueProjection("")).thenReturn(true);

            tests.add(buildDynamicTestFor(buildSinkDocumentNestedStruct(),
                    entry, BlacklistValueProjector.class, cfg));

        }

        for(Map.Entry<Set<String>,BsonDocument> entry
                : nestedValueFieldsMapWhitelist.entrySet()) {

            MongoDbSinkConnectorConfig cfg =
                    mock(MongoDbSinkConnectorConfig.class);
            when(cfg.getValueProjectionList("")).thenReturn(entry.getKey());
            when(cfg.isUsingWhitelistValueProjection("")).thenReturn(true);

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
                    .getConstructor(MongoDbSinkConnectorConfig.class, String.class).newInstance(cfg,"");

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

        BsonDocument flatKey = new BsonDocument();
        flatKey.put("_id", new BsonString("ABC-123"));
        flatKey.put("myBoolean",new BsonBoolean(true));
        flatKey.put("myInt",new BsonInt32(42));
        flatKey.put("myBytes",new BsonBinary(new byte[] {65,66,67}));
        flatKey.put("myArray", new BsonArray());

        BsonDocument flatValue = new BsonDocument();
        flatValue.put("_id", new BsonString("XYZ-789"));
        flatValue.put("myLong",new BsonInt64(42L));
        flatValue.put("myDouble",new BsonDouble(23.23d));
        flatValue.put("myString",new BsonString("BSON"));
        flatValue.put("myBytes",new BsonBinary(new byte[] {120,121,122}));
        flatValue.put("myArray", new BsonArray());

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
                    put("myArray", new BsonArray(
                            Arrays.asList(
                                    new BsonDocument() {{
                                       put("key",new BsonString("abc"));
                                       put("value",new BsonInt32(123));
                                    }},
                                    new BsonDocument() {{
                                        put("key",new BsonString("xyz"));
                                        put("value",new BsonInt32(987));
                                    }}
                                )
                            )
                    );
                }});
            }});
        }};

        return new SinkDocument(nestedKey,nestedValue);
    }

}
