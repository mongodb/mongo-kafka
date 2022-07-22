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

public class SourceTaskStatistics implements SourceTaskStatisticsMBean {

  // Timings
  private volatile long taskTimeNanos;
  private volatile long readTimeNanos;
  private volatile long externalTimeNanos;

  // Counts
  private volatile long taskInvocations;
  private volatile long returnedRecords;
  private volatile long filteredRecords;
  private volatile long successfulRecords;

  private volatile long commandsStarted;
  private volatile long successfulCommands;
  private volatile long successfulGetMoreCommands;
  private volatile long failedCommands;

  @Override
  public long getTaskTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(taskTimeNanos);
  }

  @Override
  public long getReadTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(readTimeNanos);
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
  public long getReturnedRecords() {
    return returnedRecords;
  }

  @Override
  public long getFilteredRecords() {
    return filteredRecords;
  }

  @Override
  public long getSuccessfulRecords() {
    return successfulRecords;
  }

  @Override
  public long getCommandsStarted() {
    return commandsStarted;
  }

  @Override
  public long getSuccessfulCommands() {
    return successfulCommands;
  }

  @Override
  public long getSuccessfulGetMoreCommands() {
    return successfulGetMoreCommands;
  }

  @Override
  public long getFailedCommands() {
    return failedCommands;
  }

  // Util

  public Timer startTimer() {
    return Timer.start();
  }

  // Timings

  public void taskTime(final Timer t) {
    taskTimeNanos += t.nanosElapsed();
  }

  public void readTimeNanos(final long nanoseconds) {
    readTimeNanos += nanoseconds;
  }

  public void externalTime(final Timer t) {
    externalTimeNanos += t.nanosElapsed();
  }

  // Counts

  public Timer taskInvoked() {
    taskInvocations += 1;
    return Timer.start();
  }

  public Timer commandStarted() {
    commandsStarted += 1;
    return Timer.start();
  }

  public void successfulGetMoreCommand() {
    successfulGetMoreCommands += 1;
  }

  public void successfulCommand() {
    successfulCommands += 1;
  }

  public void failedCommand() {
    failedCommands += 1;
  }

  public void returnedRecords(final int n) {
    returnedRecords += n;
  }

  public void filteredRecords(final int n) {
    filteredRecords += n;
  }

  public void successfulRecords(final int n) {
    successfulRecords += n;
  }
}
