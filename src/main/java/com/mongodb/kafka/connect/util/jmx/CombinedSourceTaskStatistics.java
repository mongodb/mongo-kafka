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

// extend SourceTaskStatistics to follow MBean conventions
public class CombinedSourceTaskStatistics extends SourceTaskStatistics {
  private final SourceTaskStatistics a;
  private final SourceTaskStatistics b;

  public CombinedSourceTaskStatistics(final SourceTaskStatistics a, final SourceTaskStatistics b) {
    this.a = a;
    this.b = b;
  }

  @Override
  public long getPollTaskTimeMs() {
    return a.getPollTaskTimeMs() + b.getPollTaskTimeMs();
  }

  @Override
  public long getInitiatingCommandElapsedTimeMs() {
    return a.getInitiatingCommandElapsedTimeMs() + b.getInitiatingCommandElapsedTimeMs();
  }

  @Override
  public long getGetMoreCommandElapsedTimeMs() {
    return a.getGetMoreCommandElapsedTimeMs() + b.getGetMoreCommandElapsedTimeMs();
  }

  @Override
  public long getTimeSpentOutsidePollTaskMs() {
    return a.getTimeSpentOutsidePollTaskMs() + b.getTimeSpentOutsidePollTaskMs();
  }

  @Override
  public long getPollTaskInvocations() {
    return a.getPollTaskInvocations() + b.getPollTaskInvocations();
  }

  @Override
  public long getReturnedRecords() {
    return a.getReturnedRecords() + b.getReturnedRecords();
  }

  @Override
  public long getFilteredRecords() {
    return a.getFilteredRecords() + b.getFilteredRecords();
  }

  @Override
  public long getSuccessfulRecords() {
    return a.getSuccessfulRecords() + b.getSuccessfulRecords();
  }

  @Override
  public long getSuccessfulInitiatingCommands() {
    return a.getSuccessfulInitiatingCommands() + b.getSuccessfulInitiatingCommands();
  }

  @Override
  public long getSuccessfulGetMoreCommands() {
    return a.getSuccessfulGetMoreCommands() + b.getSuccessfulGetMoreCommands();
  }

  @Override
  public long getFailedGetMoreCommands() {
    return a.getFailedGetMoreCommands() + b.getFailedGetMoreCommands();
  }

  @Override
  public long getFailedInitiatingCommands() {
    return a.getFailedInitiatingCommands() + b.getFailedInitiatingCommands();
  }
}
