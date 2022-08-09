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

import java.util.function.Supplier;

public abstract class MetricValue {
  private final String name;
  protected final Supplier<Long> supplier;

  private MetricValue(final String name, final Supplier<Long> supplier) {
    this.name = name;
    this.supplier = supplier;
  }

  public String getName() {
    return name;
  }

  public Long get() {
    Long value = supplier.get();
    return value == null ? 0 : value; // default to 0
  }

  public abstract MetricValue combine(final MetricValue other);

  public static final class TotalMetricValue extends MetricValue {

    public TotalMetricValue(final String name, final Supplier<Long> supplier) {
      super(name, supplier);
    }

    @Override
    public MetricValue combine(final MetricValue prior) {
      return new TotalMetricValue(this.getName(), () -> prior.get() + this.get());
    }
  }

  public static final class LatestMetricValue extends MetricValue {
    public LatestMetricValue(final String name, final Supplier<Long> supplier) {
      super(name, supplier);
    }

    @Override
    public MetricValue combine(final MetricValue prior) {
      return new LatestMetricValue(
          this.getName(),
          () -> {
            Long thisValue = supplier.get();
            // right side (this) is considered latest
            if (thisValue != null) {
              return thisValue;
            }
            return prior.get();
          });
    }
  }
}
