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

public class MetricValue {
  private final String name;
  private final Supplier<Long> supplier;

  public MetricValue(final String name, final Supplier<Long> supplier) {
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

  public MetricValue combine(final MetricValue other) {
    return new MetricValue(this.getName(), () -> this.get() + other.get());
  }
}
