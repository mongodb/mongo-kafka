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

import java.util.HashMap;
import java.util.Map;

import com.mongodb.kafka.connect.util.jmx.internal.Metric;
import com.mongodb.kafka.connect.util.jmx.internal.MongoMBean;

public class SinkTaskStatistics extends MongoMBean {

  public static final Map<String, String> DESCRIPTIONS = new HashMap<>();

  static {
    DESCRIPTIONS.put("records", "The total number of Kafka records a MongoDB sink task recieved.");
    DESCRIPTIONS.put(
        "records-successful",
        "The total number of Kafka records a MongoDB sink task successfully wrote to MongoDB.");
    DESCRIPTIONS.put(
        "records-failed",
        "The total number of Kafka records a MongoDB sink task failed to write to MongoDB.");
    DESCRIPTIONS.put(
        "latest-kafka-time-difference-ms",
        "The number of milliseconds of the most recent time difference recorded between a MongoDB sink task and Kafka. This value is calculated by subtracting the current time of the connector's clock and the timestamp of the last record the task received.");

    DESCRIPTIONS.put(
        "in-task-put",
        "The total number of times the Kafka Connect framework executed the put() method of the MongoDB sink task.");
    DESCRIPTIONS.put(
        "in-task-put-duration-ms",
        "The total number of milliseconds the Kafka Connect framework spent executing the put() method of a MongoDB sink task.");

    DESCRIPTIONS.put(
        "in-task-put-duration-over-1-ms",
        "The total number of MongoDB sink task put() method executions with a duration that exceeded 1ms.");
    DESCRIPTIONS.put(
        "in-task-put-duration-over-10-ms",
        "The total number of MongoDB sink task put() method executions with a duration that exceeded 10ms.");
    DESCRIPTIONS.put(
        "in-task-put-duration-over-100-ms",
        "The total number of MongoDB sink task put() method executions with a duration that exceeded 100ms.");
    DESCRIPTIONS.put(
        "in-task-put-duration-over-1000-ms",
        "The total number of MongoDB sink task put() method executions with a duration that exceeded 1000ms.");
    DESCRIPTIONS.put(
        "in-task-put-duration-over-10000-ms",
        "The total number of MongoDB sink task put() method executions with a duration that exceeded 10000ms.");

    DESCRIPTIONS.put(
        "in-connect-framework",
        "The total number of times code in the Kafka Connect framework executed after the first invocation of the put() method of the MongoDB sink task.");
    DESCRIPTIONS.put(
        "in-connect-framework-duration-ms",
        "The total number of milliseconds spent executing code in the Kafka Connect framework since the framework first invoked the put() method of the MongoDB sink task. This metric does not count time executing code in the MongoDB sink task towards the total.");

    DESCRIPTIONS.put(
        "in-connect-framework-duration-over-1-ms",
        "The total number of times code in the Kafka Connect framework executed for a duration that exceeded 1ms.");
    DESCRIPTIONS.put(
        "in-connect-framework-duration-over-10-ms",
        "The total number of times code in the Kafka Connect framework executed for a duration that exceeded 10ms.");
    DESCRIPTIONS.put(
        "in-connect-framework-duration-over-100-ms",
        "The total number of times code in the Kafka Connect framework executed for a duration that exceeded 100ms.");
    DESCRIPTIONS.put(
        "in-connect-framework-duration-over-1000-ms",
        "The total number of times code in the Kafka Connect framework executed for a duration that exceeded 1000ms.");
    DESCRIPTIONS.put(
        "in-connect-framework-duration-over-10000-ms",
        "The total number of times code in the Kafka Connect framework executed for a duration that exceeded 10000ms.");

    DESCRIPTIONS.put(
        "processing-phases",
        "The total number of times a MongoDB sink task executed the processing phase on a batch of records from Kafka. The processing phase of a MongoDB sink task is the set of actions that starts after records are obtained from Kafka and ends before records are written to MongoDB.");
    DESCRIPTIONS.put(
        "processing-phases-duration-ms",
        "The total number of milliseconds a MongoDB sink task spent processing records before writing them to MongoDB.");

    DESCRIPTIONS.put(
        "processing-phases-duration-over-1-ms",
        "The total number of MongoDB sink task processing phase executions with a duration that exceeded 1ms.");
    DESCRIPTIONS.put(
        "processing-phases-duration-over-10-ms",
        "The total number of MongoDB sink task processing phase executions with a duration that exceeded 10ms.");
    DESCRIPTIONS.put(
        "processing-phases-duration-over-100-ms",
        "The total number of MongoDB sink task processing phase executions with a duration that exceeded 100ms.");
    DESCRIPTIONS.put(
        "processing-phases-duration-over-1000-ms",
        "The total number of MongoDB sink task processing phase executions with a duration that exceeded 1000ms.");
    DESCRIPTIONS.put(
        "processing-phases-duration-over-10000-ms",
        "The total number of MongoDB sink task processing phase executions with a duration that exceeded 10000ms.");

    DESCRIPTIONS.put(
        "batch-writes-successful",
        "The total number of batches a MongoDB sink task successfully wrote to the MongoDB server.");
    DESCRIPTIONS.put(
        "batch-writes-successful-duration-ms",
        "The total number of milliseconds a MongoDB sink task spent succesfully writing to the MongoDB server.");

    DESCRIPTIONS.put(
        "batch-writes-successful-duration-over-1-ms",
        "The total number of successful batch writes performed by the MongoDB sink task with a duration that exceeded 1ms.");
    DESCRIPTIONS.put(
        "batch-writes-successful-duration-over-10-ms",
        "The total number of successful batch writes performed by the MongoDB sink task with a duration that exceeded 10ms.");
    DESCRIPTIONS.put(
        "batch-writes-successful-duration-over-100-ms",
        "The total number of successful batch writes performed by the MongoDB sink task with a duration that exceeded 100ms.");
    DESCRIPTIONS.put(
        "batch-writes-successful-duration-over-1000-ms",
        "The total number of successful batch writes performed by the MongoDB sink task with a duration that exceeded 1000ms.");
    DESCRIPTIONS.put(
        "batch-writes-successful-duration-over-10000-ms",
        "The total number of successful batch writes performed by the MongoDB sink task with a duration that exceeded 10000ms.");

    DESCRIPTIONS.put(
        "batch-writes-failed",
        "The total number of batches a MongoDB sink task failed to write to the MongoDB server.");
    DESCRIPTIONS.put(
        "batch-writes-failed-duration-ms",
        "The total number of milliseconds a MongoDB sink task spent unsuccessfully attempting to write batches to the MongoDB server.");

    DESCRIPTIONS.put(
        "batch-writes-failed-duration-over-1-ms",
        "The total number of failed batch writes attempted by the MongoDB sink task with a duration that exceeded 1ms.");
    DESCRIPTIONS.put(
        "batch-writes-failed-duration-over-10-ms",
        "The total number of failed batch writes attempted by the MongoDB sink task with a duration that exceeded 10ms.");
    DESCRIPTIONS.put(
        "batch-writes-failed-duration-over-100-ms",
        "The total number of failed batch writes attempted by the MongoDB sink task with a duration that exceeded 100ms.");
    DESCRIPTIONS.put(
        "batch-writes-failed-duration-over-1000-ms",
        "The total number of failed batch writes attempted by the MongoDB sink task with a duration that exceeded 1000ms.");
    DESCRIPTIONS.put(
        "batch-writes-failed-duration-over-10000-ms",
        "The total number of failed batch writes attempted by the MongoDB sink task with a duration that exceeded 10000ms.");
  }

  private final Metric records = registerTotal("records");
  private final Metric recordsSuccessful = registerTotal("records-successful");
  private final Metric recordsFailed = registerTotal("records-failed");
  private final Metric latestKafkaTimeDifferenceMs =
      registerLatest("latest-kafka-time-difference-ms");

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

  @Override
  protected String getDescription(final String name) {
    return DESCRIPTIONS.get(name);
  }
}
