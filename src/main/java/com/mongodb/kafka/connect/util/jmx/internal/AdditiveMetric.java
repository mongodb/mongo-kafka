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
package com.mongodb.kafka.connect.util.jmx.internal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * An AdditiveMetric is a measurement that may be taken periodically, with the sampled number being
 * added to some total. This produces a number of values based on that periodic sampling: the number
 * of samples, the total amount sampled, and the number of samples over some limit.
 *
 * <p>For example, we might sample the byte size of each message we receive, and the AdditiveMetric
 * will track a count of the number of times a sample has been taken, the total number of bytes
 * received, and the number of messages sampled that exceeded certain limits. Another example is the
 * amount of time that some operation takes.
 *
 * <p>The measured statistics can be compared against each other over a period of time to derive
 * other statistics. For example, the total can be divided by the count to determine the average, or
 * the number of samples over some limit may be divided by the count to determine the percentage of
 * samples over that limit.
 *
 * <p>Metrics are intended to be sampled periodically and stored by some external component. Using
 * that component to subtract some earlier time T1 from some later time T2 can provide statistics
 * over that interval of time.
 *
 * <p>The sampled numbers are generally assumed to be positive; if negative numbers are used, it may
 * become difficult for the consuming user to make certain inferences.
 */
public class AdditiveMetric implements Metric {
  private final String name;
  private final String unit;
  private final long[] limits;

  private final AtomicLong count = new AtomicLong();
  private final AtomicLong total = new AtomicLong();
  private final AtomicLong[] bins;

  public AdditiveMetric(final String name, final String unit, final long[] limits) {
    this.name = name;
    this.unit = unit;
    this.limits = limits;
    this.bins = new AtomicLong[limits.length];
    for (int i = 0; i < bins.length; i++) {
      bins[i] = new AtomicLong();
    }
  }

  @Override
  public void sample(final long n) {
    this.count.addAndGet(1);
    for (int i = 0; i < bins.length; i++) {
      if (n > limits[i]) {
        bins[i].addAndGet(1);
      }
    }
    this.total.addAndGet(n);
  }

  public void emit(final Consumer<MetricValue> consumer) {
    consumer.accept(new MetricValue.TotalMetricValue(name, count::get));
    consumer.accept(new MetricValue.TotalMetricValue(name + "-duration-" + unit, total::get));
    for (int i = 0; i < bins.length; i++) {
      int finalI = i;
      consumer.accept(
          new MetricValue.TotalMetricValue(
              name + "-duration-over-" + limits[i] + "-" + unit, () -> bins[finalI].get()));
    }
  }
}
