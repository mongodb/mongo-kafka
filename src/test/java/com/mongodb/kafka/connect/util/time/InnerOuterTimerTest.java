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

import static com.mongodb.kafka.connect.util.Assertions.fail;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.mongodb.kafka.connect.util.time.InnerOuterTimer.InnerTimer;

final class InnerOuterTimerTest {
  private static final Duration TOLERANCE = Duration.ofMillis(50);

  @Test
  @SuppressWarnings("try")
  void singleTaskExecution() throws InterruptedException {
    MutableDuration innerDuration = MutableDuration.zero();
    MutableDuration outerDuration = MutableDuration.zero();
    InnerOuterTimer timer = InnerOuterTimer.start(innerDuration::add, outerDuration::add);
    // a time gap between starting a timer and taking measurements must have no effect
    Thread.sleep(TOLERANCE.toMillis());
    Duration innerSleep = TOLERANCE.multipliedBy(4);
    try (InnerTimer automatic = timer.sampleOuter()) {
      Thread.sleep(innerSleep.toMillis());
    }
    assertEquals(Duration.ZERO, outerDuration.get());
    assertAtLeast(innerSleep, innerDuration.get());
  }

  // @Test
  @SuppressWarnings("try")
  void multipleTaskExecutions() {
    MutableDuration innerDuration = MutableDuration.zero();
    MutableDuration outerDuration = MutableDuration.zero();
    InnerOuterTimer timer = InnerOuterTimer.start(innerDuration::add, outerDuration::add);
    Timer total = Timer.start();
    for (int i = 0; i < 20_000_000; i++) {
      // measurements must be correct regardless of how little time is spent not executing the task
      try (InnerTimer automatic = timer.sampleOuter()) {
        // Measurements must be correct regardless
        // of how little time an individual task execution takes.
      }
    }
    Duration totalDuration = total.getElapsedTime();
    Duration innerPlusOuterDuration = outerDuration.get().plus(innerDuration.get());
    assertAtLeast(innerPlusOuterDuration, totalDuration);
    assertEquals(
        0.5,
        (float) innerDuration.get().toNanos() / innerPlusOuterDuration.toNanos(),
        0.1,
        () ->
            format(
                "Inner duration %s must be about half of the inner+outer %s",
                innerDuration, innerPlusOuterDuration));
  }

  private static void assertAtLeast(final Duration expected, final Duration actual) {
    if (expected.compareTo(TOLERANCE) <= 0) {
      fail(format("Expected %s must be greater than tolerance %s", expected, TOLERANCE));
    }
    assertTrue(actual.compareTo(expected) >= 0, () -> format("%s, %s", expected, actual));
    Duration overmeasure = actual.minus(expected);
    assertTrue(
        overmeasure.compareTo(TOLERANCE) < 0, () -> format("%s, %s", TOLERANCE, overmeasure));
  }

  private static final class MutableDuration {
    private Duration v;

    private MutableDuration(final Duration v) {
      this.v = v;
    }

    static MutableDuration zero() {
      return new MutableDuration(Duration.ZERO);
    }

    void add(final Duration delta) {
      this.v = this.v.plus(delta);
    }

    Duration get() {
      return v;
    }

    @Override
    public String toString() {
      return v.toString();
    }
  }
}
