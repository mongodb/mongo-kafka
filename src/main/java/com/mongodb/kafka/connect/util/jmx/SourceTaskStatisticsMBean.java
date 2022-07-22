/*
 * Copyright 2022-present MongoDB, Inc.
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

/**
 * A standard MBean interface for a {@link com.mongodb.kafka.connect.source.MongoSourceTask}.
 *
 * <p>This interface is NOT part of the public API. Be prepared for non-binary compatible changes in
 * minor releases.
 *
 * @since TODO
 */
public interface SourceTaskStatisticsMBean {

  /** @return Milliseconds spent in the task, including any sub-phases. */
  long getPollTaskTimeMs();

  /** @return Milliseconds spent reading from MongoDB. Included in {@link #getPollTaskTimeMs()}. */
  long getPollTaskReadTimeMs();

  /**
   * @return Milliseconds spent outside the task: time between the task returning and being invoked
   *     again.
   */
  long getTimeSpentOutsidePollTaskMs();

  /** @return The number of times the task was invoked by the Kafka Connect framework. */
  long getPollTaskInvocations();

  /** @return The number of records returned to the Kafka Connect framework. */
  long getReturnedRecords();

  /**
   * @return Of the returned records, the number of records the Kafka Connect framework filtered
   *     out.
   */
  long getFilteredRecords();

  /**
   * @return Of the returned records, the number of records the Kafka Connect framework successfully
   *     wrote as a Kafka event.
   */
  long getSuccessfulRecords();

  /**
   * @return The number of MongoDB commands that were started, as reported by the {@link
   *     com.mongodb.event.CommandListener}.
   */
  long getCommandsStarted();

  /**
   * @return The number of all MongoDB commands that were successful, as reported by the {@link
   *     com.mongodb.event.CommandListener}.
   */
  long getSuccessfulCommands();

  /**
   * @return The number of MongoDB getMore commands that were successful, as reported by the {@link
   *     com.mongodb.event.CommandListener}. Included in {@link #getSuccessfulCommands()}.
   */
  long getSuccessfulGetMoreCommands();

  /**
   * @return The number of all MongoDB commands that failed, as reported by the {@link
   *     com.mongodb.event.CommandListener}.
   */
  long getFailedCommands();
}
