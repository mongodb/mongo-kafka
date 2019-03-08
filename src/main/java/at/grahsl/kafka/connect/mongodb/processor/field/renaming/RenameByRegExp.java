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

package at.grahsl.kafka.connect.mongodb.processor.field.renaming;

import at.grahsl.kafka.connect.mongodb.MongoDbSinkConnectorConfig;

import java.util.List;

public class RenameByRegExp extends Renamer {

    private final List<RegExpSettings> regExpSettings;

    public RenameByRegExp(final MongoDbSinkConnectorConfig config, final String collection) {
        this(config, config.parseRenameRegExpSettings(collection), collection);
    }

    public RenameByRegExp(final MongoDbSinkConnectorConfig config, final List<RegExpSettings> regExpSettings, final String collection) {
        super(config, collection);
        this.regExpSettings = regExpSettings;
    }

    @Override
    protected boolean isActive() {
        return !regExpSettings.isEmpty();
    }

    protected String renamed(final String path, final String name) {
        String newName = name;
        for (RegExpSettings regExpSetting : regExpSettings) {
            if ((path + SUB_FIELD_DOT_SEPARATOR + name).matches(regExpSetting.getRegexp())) {
                newName = newName.replaceAll(regExpSetting.getPattern(), regExpSetting.getReplace());
            }
        }
        return newName;
    }

}
