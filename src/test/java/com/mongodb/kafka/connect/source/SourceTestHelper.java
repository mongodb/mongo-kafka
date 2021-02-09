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
package com.mongodb.kafka.connect.source;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.DATABASE_CONFIG;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

public final class SourceTestHelper {

  public static final String CLIENT_URI_DEFAULT_SETTINGS =
      "mongodb://localhost:27017,localhost:27018,localhost:27019";
  public static final String CLIENT_URI_AUTH_SETTINGS =
      "mongodb://user:pass@localhost:27017,localhost:27018,localhost:27019/kconnect";
  public static final String TEST_DATABASE = "myDB";
  public static final String TEST_COLLECTION = "myColl";

  public static Map<String, String> createConfigMap() {
    Map<String, String> map = new HashMap<>();
    map.put(DATABASE_CONFIG, TEST_DATABASE);
    map.put(COLLECTION_CONFIG, TEST_COLLECTION);
    return map;
  }

  public static Map<String, String> createConfigMap(final String json) {
    Map<String, String> map = createConfigMap();
    Document.parse(json).forEach((k, v) -> map.put(k, v.toString()));
    return map;
  }

  public static Map<String, String> createConfigMap(final String k, final String v) {
    Map<String, String> map = createConfigMap();
    map.put(k, v);
    return map;
  }

  public static MongoSourceConfig createSourceConfig() {
    return new MongoSourceConfig(createConfigMap());
  }

  public static MongoSourceConfig createSourceConfig(final String json) {
    return new MongoSourceConfig(createConfigMap(json));
  }

  public static MongoSourceConfig createSourceConfig(final String k, final String v) {
    return new MongoSourceConfig(createConfigMap(k, v));
  }

  private SourceTestHelper() {}
}
