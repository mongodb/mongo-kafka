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

package com.mongodb.kafka.connect.sink.processor.id.strategy;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.ALLOWLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.BLACKLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FieldProjectionType.BLOCKLIST;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import com.mongodb.kafka.connect.sink.processor.AllowListKeyProjector;
import com.mongodb.kafka.connect.sink.processor.AllowListValueProjector;
import com.mongodb.kafka.connect.sink.processor.BlockListKeyProjector;
import com.mongodb.kafka.connect.sink.processor.BlockListValueProjector;

@RunWith(JUnitPlatform.class)
class IdStrategyTest {
  private static final int UUID_STRING_LENGTH = 36;
  private static final int BSON_OID_STRING_LENGTH = 12;
  private static final int KAFKA_META_DATA_PARTS = 3;

  @TestFactory
  @DisplayName("test different id generation strategies")
  List<DynamicTest> testIdGenerationStrategies() {
    List<DynamicTest> idTests = new ArrayList<>();

    IdStrategy idS1 = new BsonOidStrategy();
    idTests.add(
        dynamicTest(
            BsonOidStrategy.class.getSimpleName(),
            () -> {
              BsonValue id = idS1.generateId(null, null);
              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonObjectId),
                  () ->
                      assertEquals(
                          BSON_OID_STRING_LENGTH,
                          ((BsonObjectId) id).getValue().toByteArray().length));
            }));

    UuidStrategy idS2 = new UuidStrategy();
    idTests.add(
        dynamicTest(
            UuidStrategy.class.getSimpleName(),
            () -> {
              idS2.configure(createTopicConfig());
              BsonValue id = idS2.generateId(null, null);
              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonString),
                  () -> assertEquals(UUID_STRING_LENGTH, id.asString().getValue().length()));

              idS2.configure(createTopicConfig(DOCUMENT_ID_STRATEGY_UUID_FORMAT_CONFIG, "Binary"));
              BsonValue id2 = idS2.generateId(null, null);
              assertAll(
                  "id checks",
                  () -> assertTrue(id2.isBinary()),
                  () -> assertDoesNotThrow(() -> id2.asBinary().asUuid()));
            }));

    IdStrategy idS3 = new ProvidedInKeyStrategy();
    idTests.add(
        dynamicTest(
            ProvidedStrategy.class.getSimpleName() + " in key",
            () -> {
              String idValue = "SOME_UNIQUE_ID_IN_KEY";
              SinkDocument sdWithIdInKeyDoc =
                  new SinkDocument(new BsonDocument("_id", new BsonString(idValue)), null);
              SinkDocument sdWithoutIdInKeyDoc = new SinkDocument(new BsonDocument(), null);
              SinkDocument sdWithBsonNullIdInKeyDoc =
                  new SinkDocument(new BsonDocument("_id", BsonNull.VALUE), null);
              BsonValue id = idS3.generateId(sdWithIdInKeyDoc, null);

              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonString),
                  () -> assertEquals(idValue, id.asString().getValue()));
              assertThrows(DataException.class, () -> idS3.generateId(sdWithoutIdInKeyDoc, null));
              assertThrows(
                  DataException.class, () -> idS3.generateId(sdWithBsonNullIdInKeyDoc, null));
            }));

    IdStrategy idS4 = new ProvidedInValueStrategy();
    idTests.add(
        dynamicTest(
            ProvidedStrategy.class.getSimpleName() + " in value",
            () -> {
              String idValue = "SOME_UNIQUE_ID_IN_VALUE";
              SinkDocument sdWithIdInValueDoc =
                  new SinkDocument(null, new BsonDocument("_id", new BsonString(idValue)));
              SinkDocument sdWithoutIdInValueDoc = new SinkDocument(null, new BsonDocument());
              SinkDocument sdWithBsonNullIdInValueDoc = new SinkDocument(null, new BsonDocument());
              BsonValue id = idS4.generateId(sdWithIdInValueDoc, null);

              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonString),
                  () -> assertEquals(idValue, id.asString().getValue()));
              assertThrows(DataException.class, () -> idS4.generateId(sdWithoutIdInValueDoc, null));
              assertThrows(
                  DataException.class, () -> idS4.generateId(sdWithBsonNullIdInValueDoc, null));
            }));

    IdStrategy idS5 = new KafkaMetaDataStrategy();
    idTests.add(
        dynamicTest(
            KafkaMetaDataStrategy.class.getSimpleName(),
            () -> {
              String topic = "some-topic";
              int partition = 1234;
              long offset = 9876543210L;
              SinkRecord sr = new SinkRecord(topic, partition, null, null, null, null, offset);
              BsonValue id = idS5.generateId(null, sr);

              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonString),
                  () -> {
                    String[] parts =
                        id.asString().getValue().split(KafkaMetaDataStrategy.DELIMITER);
                    assertAll(
                        "meta data checks",
                        () -> assertEquals(KAFKA_META_DATA_PARTS, parts.length),
                        () -> assertEquals(topic, parts[0]),
                        () -> assertEquals(partition, Integer.parseInt(parts[1])),
                        () -> assertEquals(offset, Long.parseLong(parts[2])));
                  });
            }));

    IdStrategy idS6 = new FullKeyStrategy();
    idTests.add(
        dynamicTest(
            FullKeyStrategy.class.getSimpleName(),
            () -> {
              BsonDocument keyDoc =
                  new BsonDocument() {
                    {
                      put("myInt", new BsonInt32(123));
                      put("myString", new BsonString("ABC"));
                    }
                  };
              SinkDocument sdWithKeyDoc = new SinkDocument(keyDoc, null);
              SinkDocument sdWithoutKeyDoc = new SinkDocument(null, null);
              BsonValue id = idS6.generateId(sdWithKeyDoc, null);

              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonDocument),
                  () -> assertEquals(keyDoc, id.asDocument()));
              assertEquals(new BsonDocument(), idS6.generateId(sdWithoutKeyDoc, null));
            }));

    IdStrategy idS7 = new UuidProvidedInKeyStrategy();
    idTests.add(
        dynamicTest(
            UuidProvidedInKeyStrategy.class.getSimpleName() + " in key",
            () -> {
              String idValue = "6d01622d-b3d5-466d-ae48-e414901af8f2";
              UUID idUuid = UUID.fromString(idValue);
              SinkDocument sdWithIdInKeyDoc =
                  new SinkDocument(new BsonDocument("_id", new BsonString(idValue)), null);
              SinkDocument sdWithoutIdInKeyDoc = new SinkDocument(new BsonDocument(), null);
              SinkDocument sdWithBsonNullIdInKeyDoc = new SinkDocument(new BsonDocument(), null);
              SinkDocument sdWithInvalidUuidInKeyDoc =
                  new SinkDocument(new BsonDocument("_id", new BsonString("invalid")), null);
              BsonValue id = idS7.generateId(sdWithIdInKeyDoc, null);

              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonBinary),
                  () -> {
                    BsonBinary bin = (BsonBinary) id;
                    UUID foundUuid = bin.asUuid(UuidRepresentation.STANDARD);
                    assertEquals(idUuid, foundUuid);
                    assertEquals(idValue, foundUuid.toString());
                  });
              assertThrows(DataException.class, () -> idS7.generateId(sdWithoutIdInKeyDoc, null));
              assertThrows(
                  DataException.class, () -> idS7.generateId(sdWithBsonNullIdInKeyDoc, null));
              assertThrows(
                  DataException.class, () -> idS7.generateId(sdWithInvalidUuidInKeyDoc, null));
            }));

    IdStrategy idS8 = new UuidProvidedInValueStrategy();
    idTests.add(
        dynamicTest(
            UuidProvidedInValueStrategy.class.getSimpleName() + " in value",
            () -> {
              String idValue = "6d01622d-b3d5-466d-ae48-e414901af8f2";
              UUID idUuid = UUID.fromString(idValue);
              SinkDocument sdWithIdInValueDoc =
                  new SinkDocument(null, new BsonDocument("_id", new BsonString(idValue)));
              SinkDocument sdWithoutIdInValueDoc = new SinkDocument(null, new BsonDocument());
              SinkDocument sdWithBsonNullIdInValueDoc = new SinkDocument(null, new BsonDocument());
              SinkDocument sdWithInvalidUuidInValueDoc =
                  new SinkDocument(null, new BsonDocument("_id", new BsonString("invalid")));
              BsonValue id = idS8.generateId(sdWithIdInValueDoc, null);

              assertAll(
                  "id checks",
                  () -> assertTrue(id instanceof BsonBinary),
                  () -> {
                    BsonBinary bin = (BsonBinary) id;
                    UUID foundUuid = bin.asUuid(UuidRepresentation.STANDARD);
                    assertEquals(idUuid, foundUuid);
                    assertEquals(idValue, foundUuid.toString());
                  });
              assertThrows(DataException.class, () -> idS8.generateId(sdWithoutIdInValueDoc, null));
              assertThrows(
                  DataException.class, () -> idS8.generateId(sdWithBsonNullIdInValueDoc, null));
              assertThrows(
                  DataException.class, () -> idS8.generateId(sdWithInvalidUuidInValueDoc, null));
            }));

    return idTests;
  }

  @Test
  @DisplayName("test PartialKeyStrategy with Block List")
  void testPartialKeyStrategyBlockList() {
    BsonDocument keyDoc = BsonDocument.parse("{keyPart1: 123, keyPart2: 'ABC', keyPart3: true}");
    BsonDocument expected = BsonDocument.parse("{keyPart2: 'ABC', keyPart3: true}");

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                "{'%s': '%s', '%s': 'keyPart1'}",
                DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG,
                BLOCKLIST,
                DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG));

    IdStrategy ids = new PartialKeyStrategy();
    ids.configure(cfg);
    SinkDocument sd = new SinkDocument(keyDoc, null);
    BsonValue id = ids.generateId(sd, null);

    assertAll(
        "id checks",
        () -> assertTrue(id instanceof BsonDocument),
        () -> assertEquals(expected, id.asDocument()));
    assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
  }

  @Test
  @DisplayName("test PartialKeyStrategy with Allow List")
  void testPartialKeyStrategyAllowList() {
    BsonDocument keyDoc = BsonDocument.parse("{keyPart1: 123, keyPart2: 'ABC', keyPart3: true}");
    BsonDocument expected = BsonDocument.parse("{keyPart1: 123}");

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                "{'%s': '%s', '%s': 'keyPart1'}",
                DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_TYPE_CONFIG,
                ALLOWLIST,
                DOCUMENT_ID_STRATEGY_PARTIAL_KEY_PROJECTION_LIST_CONFIG));

    IdStrategy ids = new PartialKeyStrategy();
    ids.configure(cfg);
    SinkDocument sd = new SinkDocument(keyDoc, null);
    BsonValue id = ids.generateId(sd, null);

    assertAll(
        "id checks",
        () -> assertTrue(id instanceof BsonDocument),
        () -> assertEquals(expected, id.asDocument()));
    assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
  }

  @Test
  @DisplayName("test PartialValueStrategy with Block List")
  void testPartialValueStrategyBlockList() {
    BsonDocument valueDoc =
        BsonDocument.parse("{valuePart1: 123, valuePart2: 'ABC', valuePart3: true}");
    BsonDocument expected = BsonDocument.parse("{valuePart2: 'ABC', valuePart3: true}");

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                "{'%s': '%s', '%s': 'valuePart1'}",
                DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_TYPE_CONFIG,
                BLOCKLIST,
                DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_LIST_CONFIG));

    IdStrategy ids = new PartialValueStrategy();
    ids.configure(cfg);
    SinkDocument sd = new SinkDocument(null, valueDoc);
    BsonValue id = ids.generateId(sd, null);

    assertAll(
        "id checks",
        () -> assertTrue(id instanceof BsonDocument),
        () -> assertEquals(expected, id.asDocument()));
    assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
  }

  @Test
  @DisplayName("test PartialValueStrategy with Allow List")
  void testPartialValueStrategyAllowList() {
    BsonDocument valueDoc =
        BsonDocument.parse("{valuePart1: 123, valuePart2: 'ABC', valuePart3: true}");
    BsonDocument expected = BsonDocument.parse("{valuePart1: 123}");

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                "{'%s': '%s', '%s': 'valuePart1'}",
                DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_TYPE_CONFIG,
                ALLOWLIST,
                DOCUMENT_ID_STRATEGY_PARTIAL_VALUE_PROJECTION_LIST_CONFIG));

    IdStrategy ids = new PartialValueStrategy();
    ids.configure(cfg);
    SinkDocument sd = new SinkDocument(null, valueDoc);
    BsonValue id = ids.generateId(sd, null);

    assertAll(
        "id checks",
        () -> assertTrue(id instanceof BsonDocument),
        () -> assertEquals(expected, id.asDocument()));
    assertEquals(new BsonDocument(), ids.generateId(new SinkDocument(null, null), null));
  }

  @Test
  @DisplayName("test ParitalKeyStrategy supports deprecated field projectors")
  void testPartialKeyStrategySupportsDeprecatedFieldProjectors() {
    String configString = "{'%s': '%s', '%s': '%s'}";
    String fieldList = "part1";
    Set<String> fieldSet = Collections.singleton(fieldList);

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                configString,
                KEY_PROJECTION_TYPE_CONFIG,
                BLACKLIST,
                KEY_PROJECTION_LIST_CONFIG,
                fieldList));
    PartialKeyStrategy blockListKeyStrategy = new PartialKeyStrategy();
    blockListKeyStrategy.configure(cfg);

    cfg =
        createTopicConfig(
            format(
                configString,
                KEY_PROJECTION_TYPE_CONFIG,
                ALLOWLIST,
                KEY_PROJECTION_LIST_CONFIG,
                fieldList));
    PartialKeyStrategy allowListKeyStrategy = new PartialKeyStrategy();
    allowListKeyStrategy.configure(cfg);

    assertAll(
        "key strategy checks",
        () -> assertTrue(blockListKeyStrategy.getFieldProjector() instanceof BlockListKeyProjector),
        () -> assertEquals(blockListKeyStrategy.getFieldProjector().getFields(), fieldSet),
        () -> assertTrue(allowListKeyStrategy.getFieldProjector() instanceof AllowListKeyProjector),
        () -> assertEquals(allowListKeyStrategy.getFieldProjector().getFields(), fieldSet));
  }

  @Test
  @DisplayName("test ParitalValueStrategy supports deprecated field projectors")
  void testPartialValueStrategySupportsDeprecatedFieldProjectors() {
    String configString = "{'%s': '%s', '%s': '%s'}";
    String fieldList = "part1";
    Set<String> fieldSet = Collections.singleton(fieldList);

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            format(
                configString,
                VALUE_PROJECTION_TYPE_CONFIG,
                BLACKLIST,
                VALUE_PROJECTION_LIST_CONFIG,
                fieldList));
    PartialValueStrategy blockListValueStrategy = new PartialValueStrategy();
    blockListValueStrategy.configure(cfg);

    cfg =
        createTopicConfig(
            format(
                configString,
                VALUE_PROJECTION_TYPE_CONFIG,
                ALLOWLIST,
                VALUE_PROJECTION_LIST_CONFIG,
                fieldList));
    PartialValueStrategy allowListValueStrategy = new PartialValueStrategy();
    allowListValueStrategy.configure(cfg);

    assertAll(
        "key strategy checks",
        () ->
            assertTrue(
                blockListValueStrategy.getFieldProjector() instanceof BlockListValueProjector),
        () -> assertEquals(blockListValueStrategy.getFieldProjector().getFields(), fieldSet),
        () ->
            assertTrue(
                allowListValueStrategy.getFieldProjector() instanceof AllowListValueProjector),
        () -> assertEquals(allowListValueStrategy.getFieldProjector().getFields(), fieldSet));
  }
}
