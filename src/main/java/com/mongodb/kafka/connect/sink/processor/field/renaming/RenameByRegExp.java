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

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.FIELD_RENAMER_REGEXP_CONFIG;
import static com.mongodb.kafka.connect.util.ConfigHelper.jsonArrayFromString;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;

public class RenameByRegExp extends Renamer {
  private final List<RegExpSettings> regExpSettings;

  public RenameByRegExp(final MongoSinkTopicConfig config) {
    super(config);
    regExpSettings = parseRenameRegExpSettings();
  }

  boolean isActive() {
    return !regExpSettings.isEmpty();
  }

  String renamed(final String path, final String name) {
    String newName = name;
    for (RegExpSettings regExpSetting : regExpSettings) {
      if ((path + SUB_FIELD_DOT_SEPARATOR + name).matches(regExpSetting.getRegexp())) {
        newName = newName.replaceAll(regExpSetting.getPattern(), regExpSetting.getReplace());
      }
    }
    return newName;
  }

  private List<RegExpSettings> parseRenameRegExpSettings() {
    String settings = getConfig().getString(FIELD_RENAMER_REGEXP_CONFIG);
    List<RegExpSettings> regExpSettings = new ArrayList<>();
    jsonArrayFromString(settings)
        .ifPresent(r -> r.forEach(d -> regExpSettings.add(new RegExpSettings(d))));
    return regExpSettings;
  }
}
