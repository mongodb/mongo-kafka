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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class LatestMetric implements Metric {
  private final String name;
  private final AtomicBoolean wasSampled = new AtomicBoolean();
  private final AtomicLong value = new AtomicLong();

  public LatestMetric(final String name) {
    this.name = name;
  }

  @Override
  public void sample(final long v) {
    // value should not be seen until wasSampled is set
    value.set(v);
    wasSampled.set(true);
  }

  @Override
  public void emit(final Consumer<MetricValue> consumer) {
    consumer.accept(
        new LatestMetricValue(
            name,
            () -> {
              if (wasSampled.get()) {
                return value.get();
              }
              return null;
            }));
  }

  private static final class LatestMetricValue extends MetricValue {
    private LatestMetricValue(final String name, final Supplier<Long> supplier) {
      super(name, supplier);
    }

    @Override
    public MetricValue combine(final MetricValue other) {
      return new LatestMetricValue(
          this.getName(),
          () -> {
            Long otherValue = other.get();
            // right side (other) is considered latest
            if (otherValue != null) {
              return otherValue;
            }
            return this.get();
          });
    }
  }
}
