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

package com.mongodb.kafka.connect.sink.namespace.mapping;

import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createTopicConfig;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

@RunWith(JUnitPlatform.class)
public class DefaultNamespaceMapperTest {

  private static final SinkRecord EMPTY_SINK_RECORD =
      new SinkRecord(TEST_TOPIC, 0, Schema.STRING_SCHEMA, "{}", Schema.STRING_SCHEMA, "{}", 1);
  private static final SinkDocument EMPTY_SINK_DOCUMENT =
      new SinkDocument(new BsonDocument(), new BsonDocument());

  @Test
  @DisplayName("test produces the expected namespace")
  void testProducesTheExpectedNamespace() {
    assertAll(
        () ->
            assertEquals(
                "myDB.topic",
                createMapper(createTopicConfig())
                    .getNamespace(EMPTY_SINK_RECORD, EMPTY_SINK_DOCUMENT)
                    .getFullName()),
        () ->
            assertEquals(
                "myDB.myColl",
                createMapper(createTopicConfig(COLLECTION_CONFIG, "myColl"))
                    .getNamespace(EMPTY_SINK_RECORD, EMPTY_SINK_DOCUMENT)
                    .getFullName()));
  }

  private NamespaceMapper createMapper(final MongoSinkTopicConfig config) {
    NamespaceMapper namespaceMapper = new DefaultNamespaceMapper();
    namespaceMapper.configure(config);
    return namespaceMapper;
  }
}
