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

package com.mongodb.kafka.connect.sink.processor.field.renaming;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_MAPPING_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.jsonArrayFromString;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.util.ConnectConfigException;

public class RenameByMapping extends Renamer {
  private Map<String, String> fieldMappings;

  public RenameByMapping(final MongoSinkTopicConfig config) {
    super(config);
    fieldMappings = parseRenameFieldnameMappings();
  }

  boolean isActive() {
    return !fieldMappings.isEmpty();
  }

  String renamed(final String path, final String name) {
    String newName = fieldMappings.get(path + SUB_FIELD_DOT_SEPARATOR + name);
    return newName != null ? newName : name;
  }

  private Map<String, String> parseRenameFieldnameMappings() {
    String settings = getConfig().getString(FIELD_RENAMER_MAPPING_CONFIG);
    Map<String, String> map = new HashMap<>();
    jsonArrayFromString(settings)
        .ifPresent(
            renames -> {
              for (Document r : renames) {
                if (!(r.containsKey("oldName") || r.containsKey("newName"))) {
                  throw new ConnectConfigException(
                      FIELD_RENAMER_MAPPING_CONFIG,
                      settings,
                      "Both oldName and newName must be mapped");
                }
                map.put(r.getString("oldName"), r.getString("newName"));
              }
            });
    return map;
  }
}
