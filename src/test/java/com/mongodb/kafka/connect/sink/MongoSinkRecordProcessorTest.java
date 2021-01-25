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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.ERRORS_TOLERANCE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_BATCH_SIZE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.NAMESPACE_MAPPER_CONFIG;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.TEST_TOPIC;
import static com.mongodb.kafka.connect.sink.SinkTestHelper.createSinkConfig;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.mongodb.kafka.connect.sink.namespace.mapping.TestNamespaceMapper;

@RunWith(JUnitPlatform.class)
class MongoSinkRecordProcessorTest {

  @Test
  @DisplayName("test mongo sink record processor batches per topic and namespace")
  void testMongoSinkRecordProcessorSortsByTopic() {
    MongoSinkConfig sinkConfig = createSinkConfig("topics", "default.topic,alt.topic");
    List<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.addAll(createSinkRecordList("default.topic", 50));
    sinkRecords.addAll(createSinkRecordList("alt.topic", 50));
    sinkRecords.addAll(createSinkRecordList("default.topic", 50));

    List<List<MongoProcessedSinkRecordData>> processedData =
        MongoSinkRecordProcessor.groupByTopicAndNamespace(sinkRecords, sinkConfig);

    assertEquals(3, processedData.size());
    assertTopicAndNamespace("default.topic", "myDB.default.topic", processedData.get(0));
    assertTopicAndNamespace("alt.topic", "myDB.alt.topic", processedData.get(1));
    assertTopicAndNamespace("default.topic", "myDB.default.topic", processedData.get(2));
  }

  @Test
  @DisplayName("test mongo sink record processor batches per topic and namespace")
  void testMongoSinkRecordProcessorSortsByTopicAndNamespace() {
    MongoSinkConfig sinkConfig =
        createSinkConfig(
            format(
                "{'topics': 'default.topic,alt.topic', '%s': '%s'}",
                NAMESPACE_MAPPER_CONFIG, TestNamespaceMapper.class.getCanonicalName()));
    List<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.addAll(createSinkRecordList("default.topic", 20));
    sinkRecords.addAll(createSinkRecordList("alt.topic", 20));

    List<List<MongoProcessedSinkRecordData>> processedData =
        MongoSinkRecordProcessor.groupByTopicAndNamespace(sinkRecords, sinkConfig);

    assertEquals(4, processedData.size());
    assertTopicAndNamespace("default.topic", "db.coll.1", processedData.get(0));
    assertTopicAndNamespace("default.topic", "db.coll.2", processedData.get(1));
    assertTopicAndNamespace("alt.topic", "db.coll.1", processedData.get(2));
    assertTopicAndNamespace("alt.topic", "db.coll.2", processedData.get(3));
  }

  @Test
  @DisplayName("test mongo sink record processor batches per topic and namespace in batches")
  void testMongoSinkRecordProcessorSortsByTopicAndNamespaceAndInBatches() {
    MongoSinkConfig sinkConfig =
        createSinkConfig(
            format(
                "{'topics': 'default.topic,alt.topic', '%s': '%s', '%s': '%s'}",
                MAX_BATCH_SIZE_CONFIG,
                5,
                NAMESPACE_MAPPER_CONFIG,
                TestNamespaceMapper.class.getCanonicalName()));
    List<SinkRecord> sinkRecords = new ArrayList<>();
    sinkRecords.addAll(createSinkRecordList("default.topic", 20));
    sinkRecords.addAll(createSinkRecordList("alt.topic", 20));

    List<List<MongoProcessedSinkRecordData>> processedData =
        MongoSinkRecordProcessor.groupByTopicAndNamespace(sinkRecords, sinkConfig);

    assertEquals(8, processedData.size());
    assertTopicAndNamespace("default.topic", "db.coll.1", processedData.get(0));
    assertTopicAndNamespace("default.topic", "db.coll.1", processedData.get(1));
    assertTopicAndNamespace("default.topic", "db.coll.2", processedData.get(2));
    assertTopicAndNamespace("default.topic", "db.coll.2", processedData.get(3));
    assertTopicAndNamespace("alt.topic", "db.coll.1", processedData.get(4));
    assertTopicAndNamespace("alt.topic", "db.coll.1", processedData.get(5));
    assertTopicAndNamespace("alt.topic", "db.coll.2", processedData.get(6));
    assertTopicAndNamespace("alt.topic", "db.coll.2", processedData.get(7));
  }

  @Test
  @DisplayName("test error tolerance is respected")
  void testErrorTolerance() {
    List<SinkRecord> sinkRecords = new ArrayList<>(createSinkRecordList(TEST_TOPIC, 50));
    sinkRecords.add(
        25, new SinkRecord(TEST_TOPIC, 0, Schema.INT32_SCHEMA, 1, Schema.INT32_SCHEMA, 1, 1));

    assertAll(
        "Ensure error tolerance is supported",
        () -> {
          List<List<MongoProcessedSinkRecordData>> processedData =
              MongoSinkRecordProcessor.groupByTopicAndNamespace(
                  sinkRecords, createSinkConfig(ERRORS_TOLERANCE_CONFIG, "all"));
          assertEquals(1, processedData.size());
          assertEquals(50, processedData.get(0).size());
          assertTopicAndNamespace(TEST_TOPIC, format("myDB.%s", TEST_TOPIC), processedData.get(0));
        },
        () ->
            assertThrows(
                DataException.class,
                () ->
                    MongoSinkRecordProcessor.groupByTopicAndNamespace(
                        sinkRecords, createSinkConfig())));
  }

  void assertTopicAndNamespace(
      final String expectedTopic,
      final String expectedNamespace,
      final List<MongoProcessedSinkRecordData> mongoProcessedSinkRecordDataList) {
    mongoProcessedSinkRecordDataList.forEach(
        processedData -> {
          assertEquals(expectedTopic, processedData.getSinkRecord().topic());
          assertEquals(expectedNamespace, processedData.getNamespace().getFullName());
        });
  }

  private List<SinkRecord> createSinkRecordList(final String topic, final int numRecords) {
    return IntStream.rangeClosed(1, numRecords)
        .boxed()
        .map(
            i ->
                new SinkRecord(
                    topic,
                    0,
                    Schema.STRING_SCHEMA,
                    format("{_id: %s, a: %s}", i, i),
                    Schema.STRING_SCHEMA,
                    format("{_id: %s, a: %s}", i, i),
                    i))
        .collect(toList());
  }
}
