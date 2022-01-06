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
package com.mongodb.kafka.connect.sink.dlq;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;

import com.mongodb.lang.Nullable;

/**
 * An exception that is meant to be reported to the DLQ via {@link ErrantRecordReporter}. The
 * {@linkplain Class#getName() class name} and {@linkplain #getMessage() message} must be treated as
 * public API. This is necessary to allow the possibility of processing the DLQ in an automated way.
 * The length of the {@linkplain #getStackTrace() stack trace} is always 0, which means that this
 * exception is suitable for reporting only situations with the stack trace being inconsequential.
 */
abstract class NoStackTraceDlqException extends DataException {
  private static final long serialVersionUID = 1L;

  NoStackTraceDlqException(@Nullable final String message) {
    super(message);
  }

  @Override
  public final Throwable fillInStackTrace() {
    return this;
  }
}
