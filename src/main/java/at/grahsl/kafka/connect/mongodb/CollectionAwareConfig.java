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

package at.grahsl.kafka.connect.mongodb;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class CollectionAwareConfig extends AbstractConfig {

    //NOTE: the merging of values() and originals() is a workaround
    //in order to allow for properties not being given in the
    //ConfigDef at compile time to be picked up and available as well...
    private final Map<String, Object> collectionAwareSettings;

    public CollectionAwareConfig(final ConfigDef definition, final Map<?, ?> originals, final boolean doLog) {
        super(definition, originals, doLog);
        collectionAwareSettings = new HashMap<>(256);
        collectionAwareSettings.putAll(values());
        collectionAwareSettings.putAll(originals());
    }

    public CollectionAwareConfig(final ConfigDef definition, final Map<?, ?> originals) {
        this(definition, originals, false);
    }

    protected Object get(final String property, final String collection) {
        String fullProperty = property + "." + collection;
        if (collectionAwareSettings.containsKey(fullProperty)) {
            return collectionAwareSettings.get(fullProperty);
        }
        return collectionAwareSettings.get(property);
    }

    String getString(final String property, final String collection) {
        if (collection == null || collection.isEmpty()) {
            return (String) get(property);
        }
        return (String) get(property, collection);
    }

    //NOTE: in the this topic aware map, everything is currently stored as
    //type String so direct casting won't work which is why the
    //*.parse*(String value) methods are to be used for now.
    Boolean getBoolean(final String property, final String collection) {
        Object obj;

        if (collection == null || collection.isEmpty()) {
            obj = get(property);
        } else {
            obj = get(property, collection);
        }

        if (obj instanceof Boolean) {
            return (Boolean) obj;
        }

        if (obj instanceof String) {
            return Boolean.parseBoolean((String) obj);
        }
        throw new ConfigException("error: unsupported property type for '" + obj + "' where Boolean expected");
    }

    Integer getInt(final String property, final String collection) {
        Object obj;
        if (collection == null || collection.isEmpty()) {
            obj = get(property);
        } else {
            obj = get(property, collection);
        }

        if (obj instanceof Integer) {
            return (Integer) obj;
        }
        if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        throw new ConfigException("error: unsupported property type for '" + obj + "' where Integer expected");
    }

}
