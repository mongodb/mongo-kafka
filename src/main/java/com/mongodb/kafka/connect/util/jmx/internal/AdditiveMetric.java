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
    consumer.accept(new MetricValue.TotalMetricValue(name + "-total-" + unit, total::get));
    for (int i = 0; i < bins.length; i++) {
      int finalI = i;
      consumer.accept(
          new MetricValue.TotalMetricValue(
              name + "-over-" + limits[i] + unit, () -> bins[finalI].get()));
    }
  }
}
