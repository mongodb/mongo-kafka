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

public class SourceTaskStatistics extends MongoMBean {

  private Metric recordsReturned = registerTotal("records-returned");
  private Metric recordsFiltered = registerTotal("records-filtered");
  private Metric recordsAcknowledged = registerTotal("records-acknowledged");
  private Metric recordsBytesRead = registerTotal("records-bytes-read");
  private Metric latestOffsetSecs = registerLatest("latest-offset-secs");

  private Metric taskInvocations = registerMs("task-invocations");
  private Metric betweenTaskInvocations = registerMs("between-task-invocations");
  private Metric successfulInitiatingCommands = registerMs("successful-initiating-commands");
  private Metric successfulGetMoreCommands = registerMs("successful-getmore-commands");
  private Metric failedInitiatingCommands = registerMs("failed-initiating-commands");
  private Metric failedGetMoreCommands = registerMs("failed-getmore-commands");

  public SourceTaskStatistics(final String name) {
    super(name);
  }

  public Metric getRecordsReturned() {
    return recordsReturned;
  }

  public Metric getRecordsFiltered() {
    return recordsFiltered;
  }

  public Metric getRecordsAcknowledged() {
    return recordsAcknowledged;
  }

  public Metric getRecordsBytesRead() {
    return recordsBytesRead;
  }

  public Metric getLatestOffsetSecs() {
    return latestOffsetSecs;
  }

  public Metric getTaskInvocations() {
    return taskInvocations;
  }

  public Metric getBetweenTaskInvocations() {
    return betweenTaskInvocations;
  }

  public Metric getSuccessfulInitiatingCommands() {
    return successfulInitiatingCommands;
  }

  public Metric getSuccessfulGetMoreCommands() {
    return successfulGetMoreCommands;
  }

  public Metric getFailedInitiatingCommands() {
    return failedInitiatingCommands;
  }

  public Metric getFailedGetMoreCommands() {
    return failedGetMoreCommands;
  }
}
