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
 * A standard MBean interface for a {@link com.mongodb.kafka.connect.sink.MongoSinkTask}.
 *
 * <p>This interface is NOT part of the public API. Be prepared for non-binary compatible changes in
 * minor releases.
 *
 * @since TODO
 */
public interface SinkTaskStatisticsMBean {

  /** @return Milliseconds spent in the task, including any sub-phases. */
  long getTaskTimeMs();

  /**
   * @return Milliseconds spent processing records before writing. Included in {@link
   *     #getTaskTimeMs()}.
   */
  long getProcessingTimeMs();

  /** @return Milliseconds spent writing to MongoDB. Included in {@link #getTaskTimeMs()}. */
  long getWriteTimeMs();

  /**
   * @return Milliseconds spent outside the task: time between the task returning and being invoked
   *     again.
   */
  long getExternalTimeMs();

  /** @return The number of times the task was invoked by the Kafka Connect framework. */
  long getTaskInvocations();

  /** @return The number of Kafka events received by the MongoDB sink task. */
  long getReceivedRecords();

  /** @return The number of times a bulk write to MongoDB was invoked. */
  long getWriteInvocations();

  /** @return The number of times a bulk write was successful. */
  long getSuccessfulWrites();

  /** @return The number of records written as part of a bulk write. */
  long getSuccessfulRecords();

  /** @return The number of times a bulk write failed to write. */
  long getFailedWrites();

  /** @return The number of records that failed to write as part of a bulk write. */
  long getFailedRecords();
}
