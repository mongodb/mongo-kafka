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

import java.util.function.Consumer;

/**
 * A measurement that may be taken periodically.
 *
 * <p>This measurement may be tracked and expressed in various ways. For example, a metric may
 * periodically measure some quantity, and track the number of measurements, the total of those
 * measurements, the latest measurement, the number of measurements exceeding some threshold, and so
 * on.
 */
public interface Metric {

  /**
   * Take the measurement.
   *
   * @param v The value of the measurement.
   */
  void sample(long v);

  /**
   * @param consumer A callback receiving a {@link MetricValue} that may be used to obtain the name
   *     and most recent value.
   */
  void emit(Consumer<MetricValue> consumer);
}
