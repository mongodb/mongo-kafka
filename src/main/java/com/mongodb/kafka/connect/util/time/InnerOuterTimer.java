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
 *
 * Original Work: Apache License, Version 2.0, Copyright 2017 Hans-Peter Grahsl.
 */
package com.mongodb.kafka.connect.util.time;

import static com.mongodb.kafka.connect.util.Assertions.assertFalse;

import java.time.Duration;
import java.util.function.Consumer;

import com.mongodb.lang.Nullable;

/**
 * A timer that measures time spent executing a sequentially repeated task (inner duration) as well
 * as time spent executing anything but this task (outer duration).
 */
public final class InnerOuterTimer {
  private final Consumer<Duration> innerConsumer;
  private final Consumer<Duration> outerConsumer;
  private final Timer timer;
  @Nullable private Duration tPreviousEndTask;

  private InnerOuterTimer(
      final Timer timer,
      final Consumer<Duration> innerConsumer,
      final Consumer<Duration> outerConsumer) {
    this.innerConsumer = innerConsumer;
    this.outerConsumer = outerConsumer;
    this.timer = timer;
  }

  public static InnerOuterTimer start(
      final Consumer<Duration> innerConsumer, final Consumer<Duration> outerConsumer) {
    return new InnerOuterTimer(Timer.start(), innerConsumer, outerConsumer);
  }

  /**
   * Samples the duration between the end of a previous execution of the task and the beginning of a
   * new task execution.
   *
   * <p>Call this method when execution reaches the beginning of the repeated task to sample the
   * outer duration. {@linkplain AutoCloseable#close() Close} the returned {@link InnerTimer} when
   * the execution reaches the end of the task.
   */
  public InnerTimer sampleOuter() {
    Duration tBeginTask = timer.getElapsedTime();
    if (tPreviousEndTask != null) {
      Duration outer = tBeginTask.minus(tPreviousEndTask);
      assertFalse(outer.isNegative());
      outerConsumer.accept(outer);
    }
    return () -> {
      Duration tEndTask = timer.getElapsedTime();
      Duration inner = tEndTask.minus(tBeginTask);
      assertFalse(inner.isNegative());
      innerConsumer.accept(inner);
      tPreviousEndTask = tEndTask;
    };
  }

  /**
   * A timer that measures time spent on a single execution of the task.
   *
   * @see #sampleOuter()
   */
  public interface InnerTimer extends AutoCloseable {
    /** Samples the duration of a single task execution. */
    void close();
  }
}
