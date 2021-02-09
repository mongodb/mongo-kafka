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
import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonNull;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.converter.FieldConverter;

public abstract class SinkFieldConverter extends FieldConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SinkFieldConverter.class);

  public SinkFieldConverter(final Schema schema) {
    super(schema);
  }

  public abstract BsonValue toBson(Object data);

  public BsonValue toBson(final Object data, final Schema fieldSchema) {
    if (!fieldSchema.isOptional()) {
      if (data == null) {
        throw new DataException("Schema not optional but data was null");
      }
      LOGGER.trace("field not optional and data is '{}'", data.toString());
      return toBson(data);
    }

    if (data != null) {
      LOGGER.trace("field optional and data is '{}'", data.toString());
      return toBson(data);
    }

    if (fieldSchema.defaultValue() != null) {
      LOGGER.trace(
          "field optional and no data but default value is '{}'",
          fieldSchema.defaultValue().toString());
      return toBson(fieldSchema.defaultValue());
    }

    LOGGER.trace("field optional, no data and no default value thus '{}'", BsonNull.VALUE);
    return BsonNull.VALUE;
  }
}
