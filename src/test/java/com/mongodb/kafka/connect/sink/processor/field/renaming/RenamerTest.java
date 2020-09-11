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

package com.mongodb.kafka.connect.sink.processor.field.renaming;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_MAPPING_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_REGEXP_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class RenamerTest {
  private final BsonDocument keyDoc =
      BsonDocument.parse(
          "{'fieldA': 'my field value', 'f2': true, 'subDoc': {'fieldX': 42}, "
              + "'my_field1': {'my_field2': 'testing rocks!'}}");
  private final BsonDocument valueDoc =
      BsonDocument.parse(
          "{'abc': 'my field value', 'f2': false, 'subDoc': {'123': 0.0},"
              + " 'foo.foo.foo': {'.blah..blah.': 23}}");

  @Test
  @DisplayName("simple field renamer test with custom field name mappings")
  void testRenamerUsingFieldnameMapping() {
    BsonDocument expectedKeyDoc =
        BsonDocument.parse(
            "{'f1': 'my field value', "
                + "'fieldB': true, 'subDoc': {'name_x': 42}, 'my_field1': {'my_field2': 'testing rocks!'}}");
    BsonDocument expectedValueDoc =
        BsonDocument.parse(
            "{'xyz': 'my field value', 'f_two': false, "
                + "'subDoc': {'789': 0.0}, 'foo.foo.foo': {'.blah..blah.': 23}}");
    MongoSinkTopicConfig cfg =
        createTopicConfig(
            FIELD_RENAMER_MAPPING_CONFIG,
            "[{'oldName':'key.fieldA','newName':'f1'}, {'oldName':'key.f2','newName':'fieldB'}, "
                + "{'oldName':'key.subDoc.fieldX','newName':'name_x'}, {'oldName':'key.fieldA','newName':'f1'}, "
                + "{'oldName':'value.abc','newName':'xyz'}, {'oldName':'value.f2','newName':'f_two'}, "
                + "{'oldName':'value.subDoc.123','newName':'789'}]");

    SinkDocument sd = new SinkDocument(keyDoc, valueDoc);
    Renamer renamer = new RenameByMapping(cfg);
    renamer.process(sd, null);

    assertAll(
        "key and value doc checks",
        () -> assertEquals(expectedKeyDoc, sd.getKeyDoc().orElse(new BsonDocument())),
        () -> assertEquals(expectedValueDoc, sd.getValueDoc().orElse(new BsonDocument())));
  }

  @Test
  @DisplayName("simple field renamer test with custom regexp settings")
  void testRenamerUsingRegExpSettings() {
    BsonDocument expectedKeyDoc =
        BsonDocument.parse(
            "{'FA': 'my field value', 'f2': true, "
                + "'subDoc': {'FX': 42}, '_F1': {'_F2': 'testing rocks!'}}");

    BsonDocument expectedValueDoc =
        BsonDocument.parse(
            "{'abc': 'my field value', 'f2': false, "
                + "'subDoc': {'123': 0.0}, 'foo_foo_foo': {'_blah__blah_': 23}}");

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            FIELD_RENAMER_REGEXP_CONFIG,
            "[{'regexp': '^key\\\\..*my.*$', 'pattern': 'my', 'replace': ''},"
                + "{'regexp': '^key\\\\..*field.$', 'pattern': 'field', 'replace': 'F'},"
                + "{'regexp': '^value\\\\..*$', 'pattern': '\\\\.', 'replace': '_'}]");

    SinkDocument sd = new SinkDocument(keyDoc, valueDoc);
    Renamer renamer = new RenameByRegExp(cfg);
    renamer.process(sd, null);

    assertAll(
        "key and value doc checks",
        () -> assertEquals(expectedKeyDoc, sd.getKeyDoc().orElse(new BsonDocument())),
        () -> assertEquals(expectedValueDoc, sd.getValueDoc().orElse(new BsonDocument())));
  }

  @Test
  @DisplayName("simple field renamer test with auto regexp escaping")
  void testRenamerUsingRegExpSettingsWithAutoEscaping() {
    BsonDocument expectedKeyDoc =
        BsonDocument.parse(
            "{'FA': 'my field value', 'f2': true, "
                + "'subDoc': {'FX': 42}, '_F1': {'_F2': 'testing rocks!'}}");

    BsonDocument expectedValueDoc =
        BsonDocument.parse(
            "{'abc': 'my field value', 'f2': false, "
                + "'subDoc': {'123': 0.0}, 'foo_foo_foo': {'_blah__blah_': 23}}");

    MongoSinkTopicConfig cfg =
        createTopicConfig(
            FIELD_RENAMER_REGEXP_CONFIG,
            "[{'regexp': '^key\\..*my.*$', 'pattern': 'my', 'replace': ''},"
                + "{'regexp': '^key\\..*field.$', 'pattern': 'field', 'replace': 'F'},"
                + "{'regexp': '^value\\..*$', 'pattern': '\\.', 'replace': '_'}]");

    SinkDocument sd = new SinkDocument(keyDoc, valueDoc);
    Renamer renamer = new RenameByRegExp(cfg);
    renamer.process(sd, null);

    assertAll(
        "key and value doc checks",
        () -> assertEquals(expectedKeyDoc, sd.getKeyDoc().orElse(new BsonDocument())),
        () -> assertEquals(expectedValueDoc, sd.getValueDoc().orElse(new BsonDocument())));
  }
}
