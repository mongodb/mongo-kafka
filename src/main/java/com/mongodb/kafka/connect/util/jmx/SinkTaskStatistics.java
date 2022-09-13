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
package com.mongodb.kafka.connect.util.jmx;

import com.mongodb.kafka.connect.util.jmx.internal.Metric;
import com.mongodb.kafka.connect.util.jmx.internal.MongoMBean;

public class SinkTaskStatistics extends MongoMBean {

  private final Metric records = registerTotal("records");
  private final Metric recordsSuccessful = registerTotal("records-successful");
  private final Metric recordsFailed = registerTotal("records-failed");
  private final Metric latestKafkaTimeDifferenceMs = registerLatest("latest-kafka-time-difference-ms");

  private final Metric inTaskPut = registerMs("in-task-put");
  private final Metric inConnectFramework = registerMs("in-connect-framework");

  private final Metric processingPhases = registerMs("processing-phases");
  private final Metric batchWritesSuccessful = registerMs("batch-writes-successful");
  private final Metric batchWritesFailed = registerMs("batch-writes-failed");

  public SinkTaskStatistics(final String name) {
    super(name);
  }

  public Metric getRecords() {
    return records;
  }

  public Metric getRecordsSuccessful() {
    return recordsSuccessful;
  }

  public Metric getRecordsFailed() {
    return recordsFailed;
  }

  public Metric getLatestKafkaTimeDifferenceMs() {
    return latestKafkaTimeDifferenceMs;
  }

  public Metric getInTaskPut() {
    return inTaskPut;
  }

  public Metric getInConnectFramework() {
    return inConnectFramework;
  }

  public Metric getProcessingPhases() {
    return processingPhases;
  }

  public Metric getBatchWritesSuccessful() {
    return batchWritesSuccessful;
  }

  public Metric getBatchWritesFailed() {
    return batchWritesFailed;
  }
}
