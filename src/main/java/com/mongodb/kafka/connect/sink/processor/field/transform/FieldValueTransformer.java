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

package com.mongodb.kafka.connect.sink.processor.field.transform;

import java.util.Map;

import org.bson.BsonValue;

/**
 * A pluggable interface for applying stateless field-level value transformations in the MongoDB Sink
 * Connector.
 *
 * <p>Implementations of this interface can perform arbitrary transformations on individual field
 * values, such as decryption, hashing, masking, or encoding. The connector instantiates the
 * configured implementation class and calls {@link #init(Map)} with the connector's configuration
 * properties, allowing access to any custom settings (e.g., encryption keys, algorithm parameters).
 *
 * <p>Example use case: a customer migrating from Oracle where certain fields are encrypted with a
 * proprietary algorithm. The customer provides a {@code FieldValueTransformer} implementation that
 * decrypts those fields before the data is written to MongoDB.
 *
 * <p>To use a custom implementation:
 *
 * <ol>
 *   <li>Implement this interface in a class with a public no-arg constructor.
 *   <li>Package it as a JAR and place it in the Kafka Connect plugin path alongside the connector
 *       JAR.
 *   <li>Configure the connector with:
 *       <pre>
 *   field.value.transformer=com.example.MyDecryptor
 *   field.value.transformer.fields=field1,field2
 *       </pre>
 * </ol>
 */
public interface FieldValueTransformer {

  /**
   * Initialize this transformer with the connector's configuration properties.
   *
   * <p>Called once during connector startup. Implementations can read any custom configuration
   * properties from the provided map (e.g., encryption keys, algorithm names).
   *
   * @param configs the connector configuration properties
   */
  void init(Map<String, String> configs);

  /**
   * Transform a single field value.
   *
   * @param fieldName the name of the field being transformed
   * @param value the original BSON value
   * @return the transformed BSON value
   */
  BsonValue transform(String fieldName, BsonValue value);
}

