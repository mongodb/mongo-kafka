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
  public long getPollTaskReadTimeMs() {
    return a.getPollTaskReadTimeMs() + b.getPollTaskReadTimeMs();
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
  public long getCommandsStarted() {
    return a.getCommandsStarted() + b.getCommandsStarted();
  }

  @Override
  public long getSuccessfulCommands() {
    return a.getSuccessfulCommands() + b.getSuccessfulCommands();
  }

  @Override
  public long getSuccessfulGetMoreCommands() {
    return a.getSuccessfulGetMoreCommands() + b.getSuccessfulGetMoreCommands();
  }

  @Override
  public long getFailedCommands() {
    return a.getFailedCommands() + b.getFailedCommands();
  }
}
