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

  private final Metric recordsReturned = registerTotal("records");
  private final Metric recordsFiltered = registerTotal("records-filtered");
  private final Metric recordsAcknowledged = registerTotal("records-acknowledged");
  private final Metric recordsReadBytes = registerTotal("mongodb-bytes-read");
  private final Metric latestOffsetSecs = registerLatest("latest-mongodb-time-difference-secs");

  private final Metric taskInvocations = registerMs("in-task-poll");
  private final Metric betweenTaskInvocations = registerMs("in-connect-framework");
  private final Metric successfulInitiatingCommands = registerMs("initial-commands-successful");
  private final Metric successfulGetMoreCommands = registerMs("getmore-commands-successful");
  private final Metric failedInitiatingCommands = registerMs("initial-commands-failed");
  private final Metric failedGetMoreCommands = registerMs("getmore-commands-failed");

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

  public Metric getRecordsReadBytes() {
    return recordsReadBytes;
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
