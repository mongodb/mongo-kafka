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
  private volatile long putTaskTimeNanos;
  private volatile long putTaskRecordProcessingTimeNanos;
  private volatile long putTaskWriteTimeNanos;
  private volatile long timeSpentOutsidePutTaskNanos;

  // Counts
  private volatile long putTaskInvocations;
  private volatile long receivedRecords;
  private volatile long successfulWrites;
  private volatile long writeInvocations;
  private volatile long successfulRecords;
  private volatile long failedWrites;
  private volatile long failedRecords;

  @Override
  public long getPutTaskTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(putTaskTimeNanos);
  }

  @Override
  public long getPutTaskRecordProcessingTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(putTaskRecordProcessingTimeNanos);
  }

  @Override
  public long getPutTaskWriteTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(putTaskWriteTimeNanos);
  }

  @Override
  public long getTimeSpentOutsidePutTaskMs() {
    return TimeUnit.NANOSECONDS.toMillis(timeSpentOutsidePutTaskNanos);
  }

  @Override
  public long getPutTaskInvocations() {
    return putTaskInvocations;
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

  // Util
  public Timer startTimer() {
    return Timer.start();
  }

  // Timings
  public void putTaskTime(final Timer t) {
    this.putTaskTimeNanos += t.nanosElapsed();
  }

  public void putTaskRecordProcessingTime(final Timer t) {
    this.putTaskRecordProcessingTimeNanos += t.nanosElapsed();
  }

  public void putTaskWriteTime(final Timer t) {
    this.putTaskWriteTimeNanos += t.nanosElapsed();
  }

  public void timeSpentOutsidePutTask(final Timer t) {
    this.timeSpentOutsidePutTaskNanos += t.nanosElapsed();
  }

  // Counts
  public Timer putTaskInvoked() {
    putTaskInvocations += 1;
    return Timer.start();
  }

  public void recordsReceived(final int n) {
    receivedRecords += n;
  }

  public Timer writeInvoked() {
    writeInvocations += 1;
    return Timer.start();
  }

  public void addSuccessfullWrite(final int batchSize) {
    successfulWrites += 1;
    successfulRecords += batchSize;
  }

  public void addFailedWrite(final int batchSize) {
    failedWrites += 1;
    failedRecords += batchSize;
  }
}
