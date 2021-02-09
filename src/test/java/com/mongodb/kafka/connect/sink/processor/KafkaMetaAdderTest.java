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

package com.mongodb.kafka.connect.sink.processor;

import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Optional;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
class KafkaMetaAdderTest {

  @Test
  @DisplayName("test KafkaMetaAdder")
  void testKafkaMetaAdder() {
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, new BsonDocument());
    new KafkaMetaAdder(createTopicConfig())
        .process(
            sinkDocWithValueDoc,
            new SinkRecord(
                TEST_TOPIC, 1, null, null, null, null, 2, 99L, TimestampType.CREATE_TIME));
    BsonDocument expected =
        BsonDocument.parse(
            "{'topic-partition-offset': 'topic-1-2', 'CREATE_TIME': {$numberLong: '99'}}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }

  @Test
  @DisplayName("test KafkaMetaAdder no timestamp")
  void testKafkaMetaAdderNoTimestamp() {
    SinkDocument sinkDocWithValueDoc = new SinkDocument(null, new BsonDocument());
    new KafkaMetaAdder(createTopicConfig())
        .process(sinkDocWithValueDoc, new SinkRecord(TEST_TOPIC, 1, null, null, null, null, 2));
    BsonDocument expected = BsonDocument.parse("{'topic-partition-offset': 'topic-1-2'}");

    assertEquals(Optional.of(expected), sinkDocWithValueDoc.getValueDoc());
  }

  @Test
  @DisplayName("test KafkaMetaAdder null values")
  void testKafkaMetaAdderNullValues() {
    SinkDocument sinkDocWithoutValueDoc = new SinkDocument(null, null);
    new KafkaMetaAdder(createTopicConfig()).process(sinkDocWithoutValueDoc, null);
    assertFalse(
        sinkDocWithoutValueDoc.getValueDoc().isPresent(), "no _id added since valueDoc was not");
  }
}
