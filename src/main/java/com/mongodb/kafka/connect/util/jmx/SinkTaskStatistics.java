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

import java.util.concurrent.TimeUnit;

public class SinkTaskStatistics implements SinkTaskStatisticsMBean {

  // Timings
  private long taskTimeNanos;
  private long processingTimeNanos;
  private long writeTimeNanos;
  private long externalTimeNanos;

  // Counts
  private long taskInvocations;
  private long receivedRecords;
  private long successfulWrites;
  private long writeInvocations;
  private long successfulRecords;
  private long failedWrites;
  private long failedRecords;

  // Util
  public Timer startTimer() {
    return Timer.start();
  }

  // Timings
  public void taskTime(Timer t) {
    this.taskTimeNanos += t.nanosElapsed();
  }

  public void processingTime(Timer t) {
    this.processingTimeNanos += t.nanosElapsed();
  }

  public void writeTime(Timer t) {
    this.writeTimeNanos += t.nanosElapsed();
  }

  public void externalTime(Timer t) {
    this.externalTimeNanos += t.nanosElapsed();
  }

  // Counts
  public Timer taskInvoked() {
    taskInvocations += 1;
    return Timer.start();
  }

  public void recordsReceived(int n) {
    receivedRecords += n;
  }

  public Timer writeInvoked() {
    writeInvocations += 1;
    return Timer.start();
  }

  public void addSuccessfullWrite(int batchSize) {
    successfulWrites += 1;
    successfulRecords += batchSize;
  }

  public void addFailedWrite(int batchSize) {
    failedWrites += 1;
    failedRecords += batchSize;
  }

  @Override
  public long getTaskTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(taskTimeNanos);
  }

  @Override
  public long getProcessingTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(processingTimeNanos);
  }

  @Override
  public long getWriteTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(writeTimeNanos);
  }

  @Override
  public long getExternalTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(externalTimeNanos);
  }

  @Override
  public long getTaskInvocations() {
    return taskInvocations;
  }

  @Override
  public long getReceivedRecords() {
    return receivedRecords;
  }

  @Override
  public long getWriteInvocations() {
    return writeInvocations;
  }

  @Override
  public long getSuccessfulWrites() {
    return successfulWrites;
  }

  @Override
  public long getSuccessfulRecords() {
    return successfulRecords;
  }

  @Override
  public long getFailedWrites() {
    return failedWrites;
  }

  @Override
  public long getFailedRecords() {
    return failedRecords;
  }
}
