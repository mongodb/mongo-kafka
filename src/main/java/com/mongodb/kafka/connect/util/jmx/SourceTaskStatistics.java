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

import java.util.concurrent.TimeUnit;

public class SourceTaskStatistics implements SourceTaskStatisticsMBean {

  private boolean disabled;

  // Timings
  private volatile long pollTaskTimeNanos;
  private volatile long initiatingCommandElapsedTime;
  private volatile long getMoreCommandElapsedTime;
  private volatile long timeSpentOutsidePollTaskNanos;

  // Counts
  private volatile long pollTaskInvocations;
  private volatile long returnedRecords;
  private volatile long filteredRecords;
  private volatile long successfulRecords;

  private volatile long successfulInitiatingCommands;
  private volatile long successfulGetMoreCommands;
  private volatile long failedInitiatingCommands;
  private volatile long failedGetMoreCommands;

  // Size
  private volatile long recordBytesRead;

  // Lag
  private volatile long lastPostBatchResumeTokenOffsetSecs;

  @Override
  public long getPollTaskTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(pollTaskTimeNanos);
  }

  @Override
  public long getInitiatingCommandElapsedTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(initiatingCommandElapsedTime);
  }

  @Override
  public long getGetMoreCommandElapsedTimeMs() {
    return TimeUnit.NANOSECONDS.toMillis(getMoreCommandElapsedTime);
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
  public long getSuccessfulInitiatingCommands() {
    return successfulInitiatingCommands;
  }

  @Override
  public long getSuccessfulGetMoreCommands() {
    return successfulGetMoreCommands;
  }

  @Override
  public long getFailedInitiatingCommands() {
    return failedInitiatingCommands;
  }

  @Override
  public long getFailedGetMoreCommands() {
    return failedGetMoreCommands;
  }

  @Override
  public long getRecordBytesRead() {
    return recordBytesRead;
  }

  @Override
  public long getLastPostBatchResumeTokenOffsetSecs() {
    return lastPostBatchResumeTokenOffsetSecs;
  }

  // Util

  public Timer startTimer() {
    return Timer.start();
  }

  // Timings

  public void pollTaskTime(final Timer t) {
    pollTaskTimeNanos += t.nanosElapsed();
  }

  public void initiatingCommandElapsedTimeNanos(final long nanoseconds) {
    initiatingCommandElapsedTime += nanoseconds;
  }

  public void getMoreCommandElapsedTimeNanos(final long nanoseconds) {
    getMoreCommandElapsedTime += nanoseconds;
  }

  public void timeSpentOutsidePollTask(final Timer t) {
    timeSpentOutsidePollTaskNanos += t.nanosElapsed();
  }

  // Counts

  public Timer taskInvoked() {
    pollTaskInvocations += 1;
    return Timer.start();
  }

  public void successfulGetMoreCommand() {
    successfulGetMoreCommands += 1;
  }

  public void successfulInitiatingCommand() {
    successfulInitiatingCommands += 1;
  }

  public void failedGetMoreCommand() {
    failedGetMoreCommands += 1;
  }

  public void failedInitiatingCommand() {
    failedInitiatingCommands += 1;
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

  public void recordBytesRead(final int bytesRead) {
    recordBytesRead += bytesRead;
  }

  public void lastPostBatchResumeTokenOffsetSecs(final long offsetSecs) {
    lastPostBatchResumeTokenOffsetSecs = offsetSecs;
  }

  /** Mark this object as no longer recording recent statistics. */
  public void disable() {
    disabled = true;
  }

  protected boolean getDisabled() {
    return disabled;
  }
}
