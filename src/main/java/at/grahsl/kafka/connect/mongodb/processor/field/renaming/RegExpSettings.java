/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2017 Hans-Peter Grahsl.
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

package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

// TODO move - configs dir?
public class RegExpSettings {

    public String regexp;
    public String pattern;
    public String replace;

    public RegExpSettings() {
    }

    public RegExpSettings(final String regexp, final String pattern, final String replace) {
        this.regexp = regexp;
        this.pattern = pattern;
        this.replace = replace;
    }

    public void setRegexp(final String regexp) {
        this.regexp = regexp;
    }

    public void setPattern(final String pattern) {
        this.pattern = pattern;
    }

    public void setReplace(final String replace) {
        this.replace = replace;
    }

    public String getRegexp() {
        return regexp;
    }

    public String getPattern() {
        return pattern;
    }

    public String getReplace() {
        return replace;
    }
}
