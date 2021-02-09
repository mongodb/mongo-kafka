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

package com.mongodb.kafka.connect.log;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.apache.log4j.spi.LoggingEvent;

/** Handy log capture class for verifying logging. */
public final class LogCapture implements AutoCloseable {

  private static final String APPENDER_NAME = "log4jRuleAppender";
  private static final Layout LAYOUT = new SimpleLayout();

  private final Logger logger;

  private final List<LoggingEvent> loggingEvents;

  public LogCapture(final Logger logger) {
    this.logger = logger;
    this.loggingEvents = new ArrayList<>();
    this.logger.addAppender(new ListAppender());
  }

  public List<LoggingEvent> getEvents() {
    return new ArrayList<>(loggingEvents);
  }

  public void reset() {
    loggingEvents.clear();
  }

  @Override
  public void close() {
    logger.removeAppender(APPENDER_NAME);
  }

  class ListAppender extends AppenderSkeleton {

    ListAppender() {
      super();
      this.setLayout(LAYOUT);
      this.setName(APPENDER_NAME);
    }

    @Override
    protected void append(final LoggingEvent event) {
      loggingEvents.add(event);
    }

    @Override
    public void close() {}

    @Override
    public boolean requiresLayout() {
      return true;
    }
  }
}
