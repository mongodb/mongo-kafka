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

package com.mongodb.kafka.connect.sink;

class RateLimitSettings {

  private final int timeoutMs;
  private final int everyN;
  private long counter = 0;

  RateLimitSettings(final Integer timeoutMs, final Integer everyN) {
    this.timeoutMs = timeoutMs;
    this.everyN = everyN;
  }

  boolean isTriggered() {
    if (timeoutMs == 0 || everyN == 0) {
      return false;
    }
    counter++;
    if (counter == everyN) {
      counter = 0;
      return true;
    }
    return false;
  }

  int getTimeoutMs() {
    return timeoutMs;
  }

  int getEveryN() {
    return everyN;
  }
}
