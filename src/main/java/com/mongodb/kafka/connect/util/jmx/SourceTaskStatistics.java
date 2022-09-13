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

  private final Metric records = registerTotal("records");
  private final Metric recordsFiltered = registerTotal("records-filtered");
  private final Metric recordsAcknowledged = registerTotal("records-acknowledged");
  private final Metric mongodbBytesRead = registerTotal("mongodb-bytes-read");
  private final Metric latestMongodbTimeDifferenceSecs = registerLatest("latest-mongodb-time-difference-secs");

  private final Metric inTaskPoll = registerMs("in-task-poll");
  private final Metric inConnectFramework = registerMs("in-connect-framework");
  private final Metric initialCommandsSuccessful = registerMs("initial-commands-successful");
  private final Metric getmoreCommandsSuccessful = registerMs("getmore-commands-successful");
  private final Metric initialCommandsFailed = registerMs("initial-commands-failed");
  private final Metric getmoreCommandsFailed = registerMs("getmore-commands-failed");

  public SourceTaskStatistics(final String name) {
    super(name);
  }

  public Metric getRecords() {
    return records;
  }

  public Metric getRecordsFiltered() {
    return recordsFiltered;
  }

  public Metric getRecordsAcknowledged() {
    return recordsAcknowledged;
  }

  public Metric getMongodbBytesRead() {
    return mongodbBytesRead;
  }

  public Metric getLatestMongodbTimeDifferenceSecs() {
    return latestMongodbTimeDifferenceSecs;
  }

  public Metric getInTaskPoll() {
    return inTaskPoll;
  }

  public Metric getInConnectFramework() {
    return inConnectFramework;
  }

  public Metric getInitialCommandsSuccessful() {
    return initialCommandsSuccessful;
  }

  public Metric getGetmoreCommandsSuccessful() {
    return getmoreCommandsSuccessful;
  }

  public Metric getInitialCommandsFailed() {
    return initialCommandsFailed;
  }

  public Metric getGetmoreCommandsFailed() {
    return getmoreCommandsFailed;
  }
}
