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
 */

package com.mongodb.kafka.connect.source.producer;

import static com.mongodb.kafka.connect.source.SourceTestHelper.createSourceConfig;
import static com.mongodb.kafka.connect.source.producer.SchemaAndValueProducers.BSON_SCHEMA_AND_VALUE_PRODUCER;
import static com.mongodb.kafka.connect.source.producer.SchemaAndValueProducers.RAW_JSON_STRING_SCHEMA_AND_VALUE_PRODUCER;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.RawBsonDocument;

@RunWith(JUnitPlatform.class)
public class SchemaAndValueProducerTest {

  private static final RawBsonDocument TEST_DOCUMENT =
      RawBsonDocument.parse(
          "{'_id': {'_id': 1}, "
              + "'operationType': 'insert', 'ns': {'db': 'myDB', 'coll': 'myColl'}, "
              + "'documentKey': {'_id': 1}, "
              + "'fullDocument': {'_id': 1, 'a': 'a', 'b': 121}}");

  @Test
  @DisplayName("test raw json string schema and value producer")
  void testRawJsonStringSchemaAndValueProducer() {
    assertEquals(
        new SchemaAndValue(Schema.STRING_SCHEMA, TEST_DOCUMENT.toJson()),
        RAW_JSON_STRING_SCHEMA_AND_VALUE_PRODUCER.create(createSourceConfig(), TEST_DOCUMENT));
  }

  @Test
  @DisplayName("test bson schema and value producer")
  void testBsonSchemaAndValueProducer() {
    SchemaAndValue actual =
        BSON_SCHEMA_AND_VALUE_PRODUCER.create(createSourceConfig(), TEST_DOCUMENT);
    assertAll(
        "Assert schema and value matches",
        () -> assertEquals(Schema.BYTES_SCHEMA.schema(), actual.schema()),
        // Ensure the data length is truncated.
        () -> assertEquals(160, ((byte[]) actual.value()).length),
        () -> assertEquals(TEST_DOCUMENT, new RawBsonDocument((byte[]) actual.value())));
  }
}
