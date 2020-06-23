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

import java.util.Objects;

import org.bson.Document;

class RegExpSettings {

  private final String regexp;
  private final String pattern;
  private final String replace;

  RegExpSettings(final Document document) {
    this(
        document.getString("regexp"), document.getString("pattern"), document.getString("replace"));
  }

  RegExpSettings(final String regexp, final String pattern, final String replace) {
    this.regexp = regexp;
    this.pattern = pattern;
    this.replace = replace;
  }

  String getRegexp() {
    return regexp;
  }

  String getPattern() {
    return pattern;
  }

  String getReplace() {
    return replace;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RegExpSettings that = (RegExpSettings) o;

    if (!Objects.equals(regexp, that.regexp)) {
      return false;
    }
    if (!Objects.equals(pattern, that.pattern)) {
      return false;
    }
    return Objects.equals(replace, that.replace);
  }

  @Override
  public int hashCode() {
    int result = regexp != null ? regexp.hashCode() : 0;
    result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
    result = 31 * result + (replace != null ? replace.hashCode() : 0);
    return result;
  }
}
