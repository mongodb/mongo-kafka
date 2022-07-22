package com.mongodb.kafka.connect.util.jmx;

public class CombinedSourceTaskStatistics implements SourceTaskStatisticsMBean {
  private final SourceTaskStatistics a;
  private final SourceTaskStatistics b;

  public CombinedSourceTaskStatistics(final SourceTaskStatistics a, final SourceTaskStatistics b) {
    this.a = a;
    this.b = b;
  }

  @Override
  public long getTaskTimeMs() {
    return a.getTaskTimeMs() + b.getTaskTimeMs();
  }

  @Override
  public long getReadTimeMs() {
    return a.getReadTimeMs() + b.getReadTimeMs();
  }

  @Override
  public long getExternalTimeMs() {
    return a.getExternalTimeMs() + b.getExternalTimeMs();
  }

  @Override
  public long getTaskInvocations() {
    return a.getTaskInvocations() + b.getTaskInvocations();
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
