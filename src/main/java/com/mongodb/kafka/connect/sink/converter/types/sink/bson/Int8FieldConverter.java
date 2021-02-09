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

import org.apache.kafka.connect.data.Schema;

import org.bson.BsonInt32;
import org.bson.BsonValue;

public class Int8FieldConverter extends SinkFieldConverter {

  public Int8FieldConverter() {
    super(Schema.INT8_SCHEMA);
  }

  @Override
  public BsonValue toBson(final Object data) {
    return new BsonInt32(((Byte) data).intValue());
  }
}
