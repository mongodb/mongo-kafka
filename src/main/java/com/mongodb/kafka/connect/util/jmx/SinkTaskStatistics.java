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

  private Metric recordsReceived = registerTotal("records-received");
  private Metric recordsSucceeded = registerTotal("records-succeeded");
  private Metric failedRecords = registerTotal("records-failed");
  private Metric latestOffsetMs = registerLatest("latest-offset-ms");

  private Metric taskInvocations = registerMs("task-invocations");
  private Metric betweenTaskInvocations = registerMs("between-task-invocations");

  private Metric recordsProcessing = registerMs("records-processing");
  private Metric successfulBatchWrites = registerMs("successful-batch-writes");
  private Metric failedBatchWrites = registerMs("failed-batch-writes");

  public SinkTaskStatistics(final String name) {
    super(name);
  }

  public Metric getRecordsReceived() {
    return recordsReceived;
  }

  public Metric getRecordsSucceeded() {
    return recordsSucceeded;
  }

  public Metric getFailedRecords() {
    return failedRecords;
  }

  public Metric getLatestOffsetMs() {
    return latestOffsetMs;
  }

  public Metric getTaskInvocations() {
    return taskInvocations;
  }

  public Metric getBetweenTaskInvocations() {
    return betweenTaskInvocations;
  }

  public Metric getRecordsProcessing() {
    return recordsProcessing;
  }

  public Metric getSuccessfulBatchWrites() {
    return successfulBatchWrites;
  }

  public Metric getFailedBatchWrites() {
    return failedBatchWrites;
  }
}
