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
 */
package com.mongodb.kafka.connect.sink.processor.id.strategy;

import static java.lang.String.format;

import java.util.UUID;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;

import com.mongodb.kafka.connect.sink.converter.SinkDocument;

class UuidProvidedStrategy extends ProvidedStrategy {

  private static final int UUID_LENGTH = 36;
  private static final int UUID_LENGTH_NO_DASHES = 32;

  UuidProvidedStrategy(final ProvidedIn where) {
    super(where);
  }

  @Override
  public BsonValue generateId(final SinkDocument doc, final SinkRecord orig) {
    BsonValue id = super.generateId(doc, orig);

    if (id.isBinary() && BsonBinarySubType.isUuid(id.asBinary().getType())) {
      return id;
    } else if (id.isString()) {
      return new BsonBinary(
          constructUuidObjectFromString(id.asString().getValue()), UuidRepresentation.STANDARD);
    }

    throw new DataException(format("UUID cannot be constructed from provided value: `%s`", id));
  }

  private UUID constructUuidObjectFromString(final String uuid) {
    try {
      if (uuid.length() == UUID_LENGTH) {
        return UUID.fromString(uuid);
      } else if (uuid.length() == UUID_LENGTH_NO_DASHES) {
        return UUID.fromString(
            uuid.replaceFirst(
                "(\\p{XDigit}{8})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}{4})(\\p{XDigit}+)",
                "$1-$2-$3-$4-$5"));
      }
    } catch (Exception e) {
      // ignore
    }

    throw new DataException(format("UUID cannot be constructed from provided value: `%s`", uuid));
  }
}
