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

package com.mongodb.kafka.connect.sink.converter.types.sink.bson;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonBinary;
import org.bson.BsonValue;

public class BytesFieldConverter extends SinkFieldConverter {

  public BytesFieldConverter() {
    super(Schema.BYTES_SCHEMA);
  }

  @Override
  public BsonValue toBson(final Object data) {

    // obviously SinkRecords may contain different types
    // to represent byte arrays
    if (data instanceof ByteBuffer) {
      return new BsonBinary(((ByteBuffer) data).array());
    }

    if (data instanceof byte[]) {
      return new BsonBinary((byte[]) data);
    }

    throw new DataException(
        "Bytes field conversion failed due to unexpected object type " + data.getClass().getName());
  }
}
