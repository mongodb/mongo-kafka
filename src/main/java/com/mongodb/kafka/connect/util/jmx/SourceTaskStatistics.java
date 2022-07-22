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
  private volatile long pollTaskTimeNanos;
  private volatile long pollTaskReadTimeNanos;
  private volatile long timeSpentOutsidePollTaskNanos;

  // Counts
  private volatile long pollTaskInvocations;
  private volatile long returnedRecords;
  private volatile long filteredRecords;
  private volatile long successfulRecords;

  private volatile long commandsStarted;
  private volatile long successfulCommands;
  private volatile long successfulGetMoreCommands;
  private volatile long failedCommands;

  @Override
  public long getPollTaskTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(pollTaskTimeNanos);
  }

  @Override
  public long getPollTaskReadTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(pollTaskReadTimeNanos);
  }

  @Override
  public long getTimeSpentOutsidePollTaskMs() {
    return TimeUnit.NANOSECONDS.toMillis(timeSpentOutsidePollTaskNanos);
  }

  @Override
  public long getPollTaskInvocations() {
    return pollTaskInvocations;
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

  public void pollTaskTime(final Timer t) {
    pollTaskTimeNanos += t.nanosElapsed();
  }

  public void pollTaskReadTimeNanos(final long nanoseconds) {
    pollTaskReadTimeNanos += nanoseconds;
  }

  public void timeSpentOutsidePollTask(final Timer t) {
    timeSpentOutsidePollTaskNanos += t.nanosElapsed();
  }

  // Counts

  public Timer taskInvoked() {
    pollTaskInvocations += 1;
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
