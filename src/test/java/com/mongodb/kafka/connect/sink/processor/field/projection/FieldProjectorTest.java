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

package com.mongodb.kafka.connect.sink.processor.field.projection;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.ALLOWLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.BLACKLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.BLOCKLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.WHITELIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.AllowListKeyProjector;
import com.mongodb.kafka.connect.sink.processor.AllowListValueProjector;
import com.mongodb.kafka.connect.sink.processor.BlacklistKeyProjector;
import com.mongodb.kafka.connect.sink.processor.BlacklistValueProjector;
import com.mongodb.kafka.connect.sink.processor.BlockListKeyProjector;
import com.mongodb.kafka.connect.sink.processor.BlockListValueProjector;
import com.mongodb.kafka.connect.sink.processor.WhitelistKeyProjector;
import com.mongodb.kafka.connect.sink.processor.WhitelistValueProjector;

@RunWith(JUnitPlatform.class)
class FieldProjectorTest {

  // flat doc field maps
  private static Map<String, BsonDocument> flatKeyFieldsMapBlockList;
  private static Map<String, BsonDocument> flatKeyFieldsMapAllowList;

  private static Map<String, BsonDocument> flatValueFieldsMapBlockList;
  private static Map<String, BsonDocument> flatValueFieldsMapAllowList;

  // nested doc field maps
  private static Map<String, BsonDocument> nestedKeyFieldsMapBlockList;
  private static Map<String, BsonDocument> nestedKeyFieldsMapAllowList;

  private static Map<String, BsonDocument> nestedValueFieldsMapBlockList;
  private static Map<String, BsonDocument> nestedValueFieldsMapAllowList;

  @BeforeAll
  static void setupFlatDocMaps() {
    // NOTE: FieldProjectors are currently implemented so that
    // a) when block listing: already present _id fields are never removed even if specified
    // b) when allow listing: already present _id fields are always kept even if not specified

    // key projection settings
    BsonDocument keyDocument1 =
        BsonDocument.parse(
            "{_id: 'ABC-123', myBoolean: true, myInt: 42, "
                + "myBytes: {$binary: 'QUJD', $type: '00'}, myArray: []}");
    BsonDocument keyDocument2 = BsonDocument.parse("{_id: 'ABC-123'}");
    BsonDocument keyDocument3 =
        BsonDocument.parse(
            "{_id: 'ABC-123', myBytes: {$binary: 'QUJD', $type: '00'}, myArray: []}");
    BsonDocument keyDocument4 =
        BsonDocument.parse(
            "{_id: 'ABC-123', myBoolean: true, myBytes: {$binary: 'QUJD', $type: '00'}, "
                + "myArray: []}");

    flatKeyFieldsMapBlockList =
        new HashMap<String, BsonDocument>() {
          {
            put("", keyDocument1);
            put("*", keyDocument2);
            put("**", keyDocument2);
            put("_id", keyDocument1);
            put("myBoolean, myInt", keyDocument3);
            put("missing1, unknown2", keyDocument1);
          }
        };

    flatKeyFieldsMapAllowList =
        new HashMap<String, BsonDocument>() {
          {
            put("", keyDocument2);
            put("*", keyDocument1);
            put("**", keyDocument1);
            put("missing1, unknown2", keyDocument2);
            put("myBoolean, myBytes, myArray", keyDocument4);
          }
        };

    // value projection settings
    BsonDocument valueDocument1 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', myLong: {$numberLong: '42'}, "
                + "myDouble: 23.23, myString: 'BSON', "
                + "myBytes: {$binary: 'eHl6', $type: '00'}, myArray: []}");
    BsonDocument valueDocument2 = BsonDocument.parse("{_id: 'XYZ-789'}");
    BsonDocument valueDocument3 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', myString: 'BSON', "
                + "myBytes: {$binary: 'eHl6', $type: '00'}, myArray: []}");
    BsonDocument valueDocument4 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', myDouble: 23.23, "
                + "myBytes: {$binary: 'eHl6', $type: '00'}, myArray: []}");

    flatValueFieldsMapBlockList =
        new HashMap<String, BsonDocument>() {
          {
            put("", valueDocument1);
            put("*", valueDocument2);
            put("**", valueDocument2);
            put("_id", valueDocument1);
            put("myLong, myDouble", valueDocument3);
            put("missing1,unknown2", valueDocument1);
          }
        };

    flatValueFieldsMapAllowList =
        new HashMap<String, BsonDocument>() {
          {
            put("", valueDocument2);
            put("*", valueDocument1);
            put("**", valueDocument1);
            put("missing1,unknown2", valueDocument2);
            put("myDouble, myBytes,myArray", valueDocument4);
          }
        };
  }

  @BeforeAll
  static void setupNestedFieldLists() {

    // NOTE: FieldProjectors are currently implemented so that
    // a) when block listing: already present _id fields are never removed even if specified
    // and
    // b) when allow listing: already present _id fields are always kept even if not specified

    BsonDocument keyDocument1 =
        BsonDocument.parse(
            "{_id: 'ABC-123', myInt: 42, "
                + "subDoc1: {myBoolean: false}, subDoc2: {myString: 'BSON2'}}");
    BsonDocument keyDocument2 =
        BsonDocument.parse(
            "{_id: 'ABC-123', "
                + "subDoc1: {myString: 'BSON1', myBoolean: false}, "
                + "subDoc2: {myString: 'BSON2', myBoolean: true}}");
    BsonDocument keyDocument3 = BsonDocument.parse("{_id: 'ABC-123'}");
    BsonDocument keyDocument4 =
        BsonDocument.parse(
            "{_id: 'ABC-123', subDoc1: {myBoolean: false}, subDoc2: {myBoolean: true}}");
    BsonDocument keyDocument5 = BsonDocument.parse("{_id: 'ABC-123', subDoc1: {}, subDoc2: {}}");
    BsonDocument keyDocument6 =
        BsonDocument.parse("{_id: 'ABC-123', myInt: 42, subDoc1: {}, subDoc2: {}}");

    nestedKeyFieldsMapBlockList =
        new HashMap<String, BsonDocument>() {
          {
            put("_id, subDoc1.myString, subDoc2.myBoolean", keyDocument1);
            put("*", keyDocument2);
            put("**", keyDocument3);
            put("*.myString", keyDocument4);
            put("*.*", keyDocument5);
          }
        };

    nestedKeyFieldsMapAllowList =
        new HashMap<String, BsonDocument>() {
          {
            put("", keyDocument3);
            put("*", keyDocument6);
          }
        };

    // Value documents
    BsonDocument valueDocument1 = BsonDocument.parse("{_id: 'XYZ-789', myBoolean: true}");
    BsonDocument valueDocument2 = BsonDocument.parse("{_id: 'XYZ-789'}");
    BsonDocument valueDocument3 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', myBoolean: true, "
                + "subDoc1: {myFieldA: 'some text', myFieldB: 12.34}}");
    BsonDocument valueDocument4 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', "
                + "subDoc1: {subSubDoc: {myString: 'some text', myInt: 0, myBoolean: false}}, subDoc2: {}}");
    BsonDocument valueDocument5 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', "
                + "subDoc1: {subSubDoc: {myString: 'some text', myInt: 0, myBoolean: false}}, "
                + "subDoc2: {subSubDoc: {myBytes: {$binary: 'eHl6', $type: '00'}, "
                + "                      myArray: [{key: 'abc', value: 123}, {key: 'xyz', value: 987}]}}}");
    BsonDocument valueDocument6 =
        BsonDocument.parse(
            "{_id: 'XYZ-789',"
                + "subDoc1: {myFieldA: 'some text', myFieldB: 12.34}, subDoc2: {myFieldA: 'some text', myFieldB: 12.34}}");
    BsonDocument valueDocument7 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', myBoolean: true,"
                + "subDoc1: {subSubDoc: {myInt: 0, myBoolean: false}}, "
                + "subDoc2: {myFieldA: 'some text', myFieldB: 12.34, subSubDoc: {}}}");
    BsonDocument valueDocument8 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', myBoolean: true, "
                + "subDoc2: {subSubDoc: {myArray: [{value: 123}, {value: 987}]}}}");
    BsonDocument valueDocument9 =
        BsonDocument.parse("{_id: 'XYZ-789', myBoolean: true, subDoc1: {}, subDoc2: {}}");
    BsonDocument valueDocument10 =
        BsonDocument.parse(
            "{_id: 'XYZ-789',"
                + "subDoc1: {myFieldA: 'some text', myFieldB: 12.34, "
                + "subSubDoc: {myString: 'some text', myInt: 0, myBoolean: false}}, "
                + "subDoc2: {subSubDoc: {myArray: [{key: 'abc', value: 123}, {key: 'xyz', value: 987}]}}}");
    BsonDocument valueDocument11 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', myBoolean: true, "
                + "subDoc1: {myFieldA: 'some text', myFieldB: 12.34, "
                + "subSubDoc: {myString: 'some text', myInt: 0, myBoolean: false}}, "
                + "subDoc2: {myFieldA: 'some text', myFieldB: 12.34, "
                + "          subSubDoc: {myBytes: {$binary: 'eHl6', $type: '00'}, "
                + "                      myArray: [{key: 'abc', value: 123}, {key: 'xyz', value: 987}]}}}");
    BsonDocument valueDocument12 =
        BsonDocument.parse(
            "{_id: 'XYZ-789', " + "subDoc2: {subSubDoc: {myArray: [{key: 'abc'}, {key: 'xyz'}]}}}");

    nestedValueFieldsMapBlockList =
        new HashMap<String, BsonDocument>() {
          {
            put("_id, subDoc1,subDoc2", valueDocument1);
            put("**", valueDocument2);
            put("subDoc1.subSubDoc,subDoc2", valueDocument3);
            put("*,subDoc1.*,subDoc2.**", valueDocument4);
            put("*.*", valueDocument5);
            put("*.subSubDoc", valueDocument6);
            put("subDoc1.*.myString,subDoc2.subSubDoc.*", valueDocument7);
            put(
                "subDoc1,subDoc2.myFieldA,subDoc2.myFieldB,subDoc2.subSubDoc.myBytes,"
                    + "subDoc2.subSubDoc.myArray.key",
                valueDocument8);
          }
        };

    nestedValueFieldsMapAllowList =
        new HashMap<String, BsonDocument>() {
          {
            put("", valueDocument2);
            put("*", valueDocument9);
            put(
                "subDoc1,subDoc1.**,subDoc2,subDoc2.subSubDoc,subDoc2.subSubDoc.myArray,"
                    + "subDoc2.subSubDoc.myArray.*",
                valueDocument10);
            put("**", valueDocument11);
            put(
                "subDoc2,subDoc2.subSubDoc,subDoc2.subSubDoc.myArray,"
                    + "subDoc2.subSubDoc.myArray.key",
                valueDocument12);
          }
        };
  }

  @TestFactory
  @DisplayName("testing different projector settings for flat structure")
  List<DynamicTest> testProjectorSettingsOnFlatStructure() {
    List<DynamicTest> tests = new ArrayList<>();

    for (Map.Entry<String, BsonDocument> entry : flatKeyFieldsMapBlockList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  BLOCKLIST,
                  KEY_PROJECTION_TYPE_CONFIG,
                  KEY_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentFlatStruct(), new BlockListKeyProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : flatKeyFieldsMapAllowList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  ALLOWLIST,
                  KEY_PROJECTION_TYPE_CONFIG,
                  KEY_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentFlatStruct(), new AllowListKeyProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : flatValueFieldsMapBlockList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  BLOCKLIST,
                  VALUE_PROJECTION_TYPE_CONFIG,
                  VALUE_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentFlatStruct(), new BlockListValueProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : flatValueFieldsMapAllowList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  ALLOWLIST,
                  VALUE_PROJECTION_TYPE_CONFIG,
                  VALUE_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentFlatStruct(), new AllowListValueProjector(cfg)));
    }

    return tests;
  }

  @TestFactory
  @DisplayName("testing different projector settings for nested structure")
  List<DynamicTest> testProjectorSettingsOnNestedStructure() {
    List<DynamicTest> tests = new ArrayList<>();

    for (Map.Entry<String, BsonDocument> entry : nestedKeyFieldsMapBlockList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  BLOCKLIST,
                  KEY_PROJECTION_TYPE_CONFIG,
                  KEY_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentNestedStruct(), new BlockListKeyProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : nestedKeyFieldsMapAllowList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  ALLOWLIST,
                  KEY_PROJECTION_TYPE_CONFIG,
                  KEY_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentNestedStruct(), new WhitelistKeyProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : nestedValueFieldsMapBlockList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  BLOCKLIST,
                  VALUE_PROJECTION_TYPE_CONFIG,
                  VALUE_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentNestedStruct(), new BlacklistValueProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : nestedValueFieldsMapAllowList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  ALLOWLIST,
                  VALUE_PROJECTION_TYPE_CONFIG,
                  VALUE_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentNestedStruct(), new WhitelistValueProjector(cfg)));
    }

    return tests;
  }

  @TestFactory
  @DisplayName("testing different projector settings for flat structure with deprecated settings")
  List<DynamicTest> testProjectorSettingsDeprecated() {
    List<DynamicTest> tests = new ArrayList<>();

    for (Map.Entry<String, BsonDocument> entry : flatKeyFieldsMapBlockList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  BLACKLIST,
                  KEY_PROJECTION_TYPE_CONFIG,
                  KEY_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentFlatStruct(), new BlacklistKeyProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : flatKeyFieldsMapAllowList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  WHITELIST,
                  KEY_PROJECTION_TYPE_CONFIG,
                  KEY_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentFlatStruct(), new WhitelistKeyProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : nestedValueFieldsMapBlockList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  BLACKLIST,
                  VALUE_PROJECTION_TYPE_CONFIG,
                  VALUE_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentNestedStruct(), new BlacklistValueProjector(cfg)));
    }

    for (Map.Entry<String, BsonDocument> entry : nestedValueFieldsMapAllowList.entrySet()) {
      MongoSinkTopicConfig cfg =
          createTopicConfig(
              format(
                  "{'%s': '%s', '%s': '%s'}",
                  WHITELIST,
                  VALUE_PROJECTION_TYPE_CONFIG,
                  VALUE_PROJECTION_LIST_CONFIG,
                  entry.getKey()));
      tests.add(
          buildDynamicTestFor(
              entry, buildSinkDocumentNestedStruct(), new WhitelistValueProjector(cfg)));
    }

    return tests;
  }

  private static DynamicTest buildDynamicTestFor(
      final Map.Entry<String, BsonDocument> entry,
      final SinkDocument doc,
      final FieldProjector fp) {
    return dynamicTest(
        fp.getClass().getSimpleName() + " with: " + entry.getKey(),
        () -> {
          fp.process(doc, null);
          assertEquals(entry.getValue(), extractBsonDocument(doc, fp));
        });
  }

  private static BsonDocument extractBsonDocument(
      final SinkDocument doc, final FieldProjector which) {
    if (which instanceof BlockListKeyProjector || which instanceof AllowListKeyProjector) {
      return doc.getKeyDoc()
          .orElseThrow(() -> new DataException("the needed BSON key doc was not present"));
    }
    if (which instanceof BlockListValueProjector || which instanceof AllowListValueProjector) {
      return doc.getValueDoc()
          .orElseThrow(() -> new DataException("the needed BSON value was not present"));
    }
    throw new IllegalArgumentException("unexpected projector type " + which.getClass().getName());
  }

  private static SinkDocument buildSinkDocumentFlatStruct() {

    BsonDocument flatKey =
        BsonDocument.parse(
            "{_id: 'ABC-123', myBoolean: true, myInt: 42, "
                + "myBytes: {$binary: 'QUJD', $type: '00'}, myArray: []}");
    BsonDocument flatValue =
        BsonDocument.parse(
            "{ _id: 'XYZ-789', myLong: { $numberLong: '42' }, myDouble: 23.23, myString: 'BSON', "
                + "myBytes: { $binary: 'eHl6', $type: '00' }, myArray: [] }");
    return new SinkDocument(flatKey, flatValue);
  }

  private static SinkDocument buildSinkDocumentNestedStruct() {
    BsonDocument nestedKey =
        BsonDocument.parse(
            "{ _id: 'ABC-123', myInt: 42, "
                + "subDoc1: { myString: 'BSON1', myBoolean: false }, subDoc2: { myString: 'BSON2', myBoolean: true } }");
    BsonDocument nestedValue =
        BsonDocument.parse(
            "{ _id: 'XYZ-789', myBoolean: true, "
                + "subDoc1: { myFieldA: 'some text', myFieldB: 12.34, subSubDoc: { myString: 'some text', myInt: 0, myBoolean: false } }, "
                + "subDoc2: { myFieldA: 'some text', myFieldB: 12.34, "
                + "           subSubDoc: { myBytes: { $binary: 'eHl6', $type: '00' }, "
                + "                        myArray: [{ key: 'abc', value: 123 }, { key: 'xyz', value: 987 }] } } }");
    return new SinkDocument(nestedKey, nestedValue);
  }
}
