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

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;

import java.util.Map;

public class RenameByRegExp extends Renamer {

    private Map<String, PatternReplace> fieldRegExps;

    public static class PatternReplace {

        public final String pattern;
        public final String replace;

        public PatternReplace(String pattern, String replace) {
            this.pattern = pattern;
            this.replace = replace;
        }

        @Override
        public String toString() {
            return "PatternReplace{" +
                    "pattern='" + pattern + '\'' +
                    ", replace='" + replace + '\'' +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PatternReplace that = (PatternReplace) o;

            if (pattern != null ? !pattern.equals(that.pattern) : that.pattern != null) return false;
            return replace != null ? replace.equals(that.replace) : that.replace == null;
        }

        @Override
        public int hashCode() {
            int result = pattern != null ? pattern.hashCode() : 0;
            result = 31 * result + (replace != null ? replace.hashCode() : 0);
            return result;
        }
    }

    public RenameByRegExp(MongoDbSinkConnectorConfig config, String collection) {
        super(config,collection);
        this.fieldRegExps = config.parseRenameRegExpSettings(collection);
    }

    public RenameByRegExp(MongoDbSinkConnectorConfig config,
                            Map<String, PatternReplace> fieldRegExps, String collection) {
        super(config,collection);
        this.fieldRegExps = fieldRegExps;
    }

    @Override
    protected boolean isActive() {
        return !fieldRegExps.isEmpty();
    }

    protected String renamed(String path, String name) {
        String newName = name;
        for(Map.Entry<String,PatternReplace> e : fieldRegExps.entrySet()) {
            if((path+SUB_FIELD_DOT_SEPARATOR+name).matches(e.getKey())) {
                newName = newName.replaceAll(e.getValue().pattern,e.getValue().replace);
            }
        }
        return newName;
    }

}
