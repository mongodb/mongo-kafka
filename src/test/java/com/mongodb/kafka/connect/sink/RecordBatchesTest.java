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

import static com.google.common.collect.Lists.partition;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

@RunWith(JUnitPlatform.class)
class RecordBatchesTest {
  private static final List<List<SinkRecord>> LIST_INITIAL_EMPTY = new ArrayList<>();
  private static final int NUM_FAKE_RECORDS = 50;

  @BeforeAll
  static void setupVerificationList() {
    LIST_INITIAL_EMPTY.add(new ArrayList<>());
  }

  @TestFactory
  @DisplayName("test batching with different config params for max.batch.size")
  Stream<DynamicTest> testBatchingWithDifferentConfigsForBatchSize() {

    return Stream.iterate(0, r -> r + 1)
        .limit(NUM_FAKE_RECORDS + 1)
        .map(
            batchSize ->
                dynamicTest(
                    "test batching for "
                        + NUM_FAKE_RECORDS
                        + " records with batchsize="
                        + batchSize,
                    () -> {
                      RecordBatches batches = new RecordBatches(batchSize, NUM_FAKE_RECORDS);
                      assertEquals(LIST_INITIAL_EMPTY, batches.getBufferedBatches());
                      List<SinkRecord> recordList =
                          createSinkRecordList("foo", 0, 0, NUM_FAKE_RECORDS);
                      recordList.forEach(batches::buffer);
                      List<List<SinkRecord>> batchedList =
                          partition(recordList, batchSize > 0 ? batchSize : recordList.size());
                      assertEquals(batchedList, batches.getBufferedBatches());
                    }));
  }

  private static List<SinkRecord> createSinkRecordList(
      final String topic, final int partition, final int beginOffset, final int size) {
    List<SinkRecord> list = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      list.add(new SinkRecord(topic, partition, null, null, null, null, beginOffset + i));
    }
    return list;
  }
}
