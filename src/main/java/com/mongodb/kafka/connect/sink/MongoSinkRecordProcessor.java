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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.MAX_BATCH_SIZE_CONFIG;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class MongoSinkRecordProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoSinkRecordProcessor.class);

  static List<List<MongoProcessedSinkRecordData>> groupByTopicAndNamespace(
      final Collection<SinkRecord> records, final MongoSinkConfig sinkConfig) {
    LOGGER.debug("Number of sink records to process: {}", records.size());
    LOGGER.debug("Buffering sink records into grouped namespace batches");

    List<List<MongoProcessedSinkRecordData>> orderedProcessedSinkRecordData = new ArrayList<>();
    List<MongoProcessedSinkRecordData> currentGroup = new ArrayList<>();
    MongoProcessedSinkRecordData previous = null;

    for (SinkRecord record : records) {
      MongoProcessedSinkRecordData processedData =
          new MongoProcessedSinkRecordData(record, sinkConfig);

      if (!processedData.canProcess()) {
        continue;
      }

      if (previous == null) {
        previous = processedData;
      }

      int maxBatchSize = processedData.getConfig().getInt(MAX_BATCH_SIZE_CONFIG);
      if (maxBatchSize > 0 && currentGroup.size() == maxBatchSize
          || !previous.getSinkRecord().topic().equals(processedData.getSinkRecord().topic())
          || !previous.getNamespace().equals(processedData.getNamespace())) {

        orderedProcessedSinkRecordData.add(currentGroup);
        currentGroup = new ArrayList<>();
      }
      previous = processedData;
      currentGroup.add(processedData);
    }

    if (!currentGroup.isEmpty()) {
      orderedProcessedSinkRecordData.add(currentGroup);
    }
    return orderedProcessedSinkRecordData;
  }

  private MongoSinkRecordProcessor() {}
}
