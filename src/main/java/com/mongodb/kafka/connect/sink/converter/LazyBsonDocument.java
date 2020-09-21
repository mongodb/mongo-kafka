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

package com.mongodb.kafka.connect.sink.converter;

import static java.lang.String.format;

import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

public class LazyBsonDocument extends BsonDocument {
  private static final long serialVersionUID = 1L;

  private transient SinkRecord record;
  private transient Type dataType;
  private transient BiFunction<Schema, Object, BsonDocument> converter;

  private BsonDocument unwrapped;

  enum Type {
    KEY,
    VALUE
  }

  /**
   * Construct a new instance with the given suppler of the document.
   *
   * @param record the sink record to convert
   * @param converter the converter for the sink record
   */
  public LazyBsonDocument(
      final SinkRecord record,
      final Type dataType,
      final BiFunction<Schema, Object, BsonDocument> converter) {
    if (record == null) {
      throw new IllegalArgumentException("SinkRecord can not be null");
    } else if (converter == null) {
      throw new IllegalArgumentException("BiFunction can not be null");
    }
    this.record = record;
    this.dataType = dataType;
    this.converter = converter;
  }

  @Override
  public int size() {
    return getUnwrapped().size();
  }

  @Override
  public boolean isEmpty() {
    return getUnwrapped().isEmpty();
  }

  @Override
  public boolean containsKey(final Object key) {
    return getUnwrapped().containsKey(key);
  }

  @Override
  public boolean containsValue(final Object value) {
    return getUnwrapped().containsValue(value);
  }

  @Override
  public BsonValue get(final Object key) {
    return getUnwrapped().get(key);
  }

  @Override
  public BsonValue put(final String key, final BsonValue value) {
    return getUnwrapped().put(key, value);
  }

  @Override
  public BsonValue remove(final Object key) {
    return getUnwrapped().remove(key);
  }

  @Override
  public void putAll(final Map<? extends String, ? extends BsonValue> m) {
    getUnwrapped().putAll(m);
  }

  @Override
  public void clear() {
    getUnwrapped().clear();
  }

  @Override
  public Set<String> keySet() {
    return getUnwrapped().keySet();
  }

  @Override
  public Collection<BsonValue> values() {
    return getUnwrapped().values();
  }

  @Override
  public Set<Entry<String, BsonValue>> entrySet() {
    return getUnwrapped().entrySet();
  }

  @Override
  public boolean equals(final Object o) {
    return getUnwrapped().equals(o);
  }

  @Override
  public int hashCode() {
    return getUnwrapped().hashCode();
  }

  @Override
  public String toString() {
    return getUnwrapped().toString();
  }

  @Override
  public BsonDocument clone() {
    return getUnwrapped().clone();
  }

  private BsonDocument getUnwrapped() {
    if (unwrapped == null) {
      switch (dataType) {
        case KEY:
          try {
            unwrapped = converter.apply(record.keySchema(), record.key());
          } catch (Exception e) {
            throw new DataException(
                format("Could not convert key `%s` into a BsonDocument.", record.key()), e);
          }
          break;
        case VALUE:
          try {
            unwrapped = converter.apply(record.valueSchema(), record.value());
          } catch (Exception e) {
            throw new DataException(
                format("Could not convert value `%s` into a BsonDocument.", record.value()), e);
          }
          break;
        default:
          throw new DataException(format("Unknown data type %s.", dataType));
      }
    }
    return unwrapped;
  }

  // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/output.html
  private Object writeReplace() {
    return getUnwrapped();
  }

  // see https://docs.oracle.com/javase/6/docs/platform/serialization/spec/input.html
  private void readObject(final ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("Proxy required");
  }
}
