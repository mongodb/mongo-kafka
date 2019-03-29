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
package com.mongodb.kafka.connect.sink.processor.field.projection;

import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.KEY_PROJECTION_TYPE_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_LIST_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkTopicConfig.VALUE_PROJECTION_TYPE_CONFIG;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.processor.PostProcessor;

public abstract class FieldProjector extends PostProcessor {
    private static final String FIELD_LIST_SPLIT_EXPR = "\\s*,\\s*";
    static final String SINGLE_WILDCARD = "*";
    static final String DOUBLE_WILDCARD = "**";
    static final String SUB_FIELD_DOT_SEPARATOR = ".";

    private final Set<String> fields;

    public FieldProjector(final MongoSinkTopicConfig config, final Set<String> fields) {
        super(config);
        this.fields = fields;
    }

    public Set<String> getFields() {
        return fields;
    }

    protected abstract void doProjection(String field, BsonDocument doc);

    protected static Set<String> getKeyFields(final AbstractConfig config) {
        return buildProjectionList(config.getString(KEY_PROJECTION_TYPE_CONFIG), config.getString(KEY_PROJECTION_LIST_CONFIG));
    }

    protected static Set<String> getValueFields(final AbstractConfig config) {
        return buildProjectionList(config.getString(VALUE_PROJECTION_TYPE_CONFIG), config.getString(VALUE_PROJECTION_LIST_CONFIG));
    }

    private static Set<String> buildProjectionList(final String projectionType, final String fieldList) {
        if (projectionType.equalsIgnoreCase(MongoSinkTopicConfig.FieldProjectionType.BLACKLIST.name())) {
            return new HashSet<>(toList(fieldList));
        } else if (projectionType.equalsIgnoreCase(MongoSinkTopicConfig.FieldProjectionType.WHITELIST.name())) {
            //NOTE: for sub document notation all left prefix bound paths are created
            //which allows for easy recursion mechanism to whitelist nested doc fields

            HashSet<String> whitelistExpanded = new HashSet<>();
            List<String> fields = toList(fieldList);

            for (String f : fields) {
                String entry = f;
                whitelistExpanded.add(entry);
                while (entry.contains(".")) {
                    entry = entry.substring(0, entry.lastIndexOf("."));
                    if (!entry.isEmpty()) {
                        whitelistExpanded.add(entry);
                    }
                }
            }
            return whitelistExpanded;
        } else {
            return new HashSet<>();
        }
    }

    private static List<String> toList(final String value) {
        return Arrays.stream(value.trim().split(FIELD_LIST_SPLIT_EXPR)).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

}
