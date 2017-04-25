/*
 * Copyright (c) 2017. Hans-Peter Grahsl (grahslhp@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

public class RegExpSettings {

    public String regexp;
    public String pattern;
    public String replace;

    public RegExpSettings() {}

    public RegExpSettings(String regexp, String pattern, String replace) {
        this.regexp = regexp;
        this.pattern = pattern;
        this.replace = replace;
    }

    @Override
    public String toString() {
        return "RegExpSettings{" +
                "regexp='" + regexp + '\'' +
                ", pattern='" + pattern + '\'' +
                ", replace='" + replace + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RegExpSettings that = (RegExpSettings) o;

        if (regexp != null ? !regexp.equals(that.regexp) : that.regexp != null) return false;
        if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) return false;
        return replace != null ? replace.equals(that.replace) : that.replace == null;
    }

    @Override
    public int hashCode() {
        int result = regexp != null ? regexp.hashCode() : 0;
        result = 31 * result + (pattern != null ? pattern.hashCode() : 0);
        result = 31 * result + (replace != null ? replace.hashCode() : 0);
        return result;
    }
}
