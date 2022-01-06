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

import java.util.List;

import org.apache.kafka.connect.sink.SinkRecord;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;

/**
 * This exception signifies that writing a {@link SinkRecord} was not attempted by a MongoDB server,
 * e.g., because writes where done via an {@linkplain BulkWriteOptions#isOrdered() ordered}
 * {@linkplain MongoCollection#bulkWrite(List, BulkWriteOptions) bulk write}, and the server failed
 * to write a preceding {@link SinkRecord}.
 */
public final class WriteSkippedException extends NoStackTraceDlqException {
  private static final long serialVersionUID = 1L;

  public WriteSkippedException() {
    super(null);
  }
}
