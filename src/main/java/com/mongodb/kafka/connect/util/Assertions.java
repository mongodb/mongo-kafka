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
package com.mongodb.kafka.connect.util;

import com.mongodb.lang.Nullable;

/**
 * All methods throw {@link AssertionError} and should be used to check conditions which may be
 * violated if and only if the driver code is incorrect. The intended usage of these methods is the
 * same as of the <a
 * href="https://docs.oracle.com/javase/8/docs/technotes/guides/language/assert.html">Java {@code
 * assert} statement</a>. The reason for not using the {@code assert} statements is that they are
 * not always enabled. We prefer having internal checks always done at the cost of our code doing a
 * relatively small amount of additional work in production. The {@code assert...} methods return
 * values to open possibilities of being used fluently.
 */
public final class Assertions {
  /**
   * @param value A value to check.
   * @param <T> The type of {@code value}.
   * @return {@code value}
   * @throws AssertionError If {@code value} is {@code null}.
   */
  public static <T> T assertNotNull(@Nullable final T value) throws AssertionError {
    if (value == null) {
      throw new AssertionError();
    }
    return value;
  }

  /**
   * @param value A value to check.
   * @return {@code true}.
   * @throws AssertionError If {@code value} is {@code false}.
   */
  public static boolean assertTrue(final boolean value) throws AssertionError {
    if (!value) {
      throw new AssertionError();
    }
    return true;
  }

  private Assertions() {}
}
