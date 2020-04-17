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
 * Original Work: Apache License, Version 2.0, Copyright 2018 Confluent Inc. EnumValidatorAndRecommender
 */

package com.mongodb.kafka.connect.util;

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPIC_OVERRIDE_DOC;
import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public final class Validators {

    public interface ValidatorWithOperators extends ConfigDef.Validator {
        default ValidatorWithOperators or(final ValidatorWithOperators other) {
            return withStringDef(format("%s OR %s", this.toString(), other.toString()), (name, value) -> {
                try {
                    this.ensureValid(name, value);
                } catch (ConfigException e) {
                    other.ensureValid(name, value);
                }
            });
        }
    }

    public static ValidatorWithOperators emptyString() {
        return withStringDef("An empty string", (name, value) -> {
            // value type already validated when parsed as String, hence ignoring ClassCastException
            if (!((String) value).isEmpty()) {
                throw new ConfigException(name, value, "Not empty");
            }
        });
    }

    public static ValidatorWithOperators matching(final Pattern pattern) {
        return withStringDef(format("A string matching `%s`", pattern), (name, value) -> {
            // type already validated when parsing config, hence ignoring ClassCastException
            if (!pattern.matcher((String) value).matches()) {
                throw new ConfigException(name, value, "Does not match: " + pattern);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public static ValidatorWithOperators listMatchingPattern(final Pattern pattern) {
        return withStringDef(format("A list matching: `%s`", pattern), (name, value) -> ((List) value).forEach(v -> {
            if (!pattern.matcher((String) v).matches()) {
                throw new ConfigException(name, value, "Contains an invalid value. Does not match: " + pattern);
            }
        }));
    }

    public static ValidatorWithOperators isAValidRegex() {
        return withStringDef("A valid regex", ((name, value) -> {
            try {
                Pattern.compile((String) value);
            } catch (Exception e) {
                throw new ConfigException(name, value, "Invalid regex: " + e.getMessage());
            }
        }));
    }

    public static ValidatorWithOperators topicOverrideValidator() {
        return withStringDef("Topic override", (name, value) -> {
            if (!((String) value).isEmpty()) {
                throw new ConfigException(name, value, "This configuration shouldn't be set directly. See the documentation about how to "
                        + "configure topic based overrides.\n" + TOPIC_OVERRIDE_DOC
                );
            }
        });
    }

    public static ValidatorWithOperators errorCheckingValueValidator(final String validValuesString, final Consumer<String> consumer) {
        return withStringDef(validValuesString, ((name, value) -> {
            try {
                consumer.accept((String) value);
            } catch (Exception e) {
                throw new ConfigException(name, value, e.getMessage());
            }
        }));
    }

    public static ValidatorWithOperators withStringDef(final String validatorString, final ConfigDef.Validator validator) {
        return new ValidatorWithOperators() {
            @Override
            public void ensureValid(final String name, final Object value) {
                validator.ensureValid(name, value);
            }

            @Override
            public String toString() {
                return validatorString;
            }
        };
    }

    public static final class EnumValidatorAndRecommender implements ValidatorWithOperators, ConfigDef.Recommender {
        private final List<String> values;

        private EnumValidatorAndRecommender(final List<String> values) {
            this.values = values;
        }

        public static <E> EnumValidatorAndRecommender in(final E[] enumerators) {
            return in(enumerators, Object::toString);
        }

        public static <E> EnumValidatorAndRecommender in(final E[] enumerators, final Function<E, String> mapper) {
            final List<String> values = new ArrayList<>(enumerators.length);
            for (E e : enumerators) {
                values.add(mapper.apply(e).toLowerCase());
            }
            return new EnumValidatorAndRecommender(values);
        }

        @Override
        public void ensureValid(final String key, final Object value) {
            String enumValue = (String) value;
            if (!values.contains(enumValue.toLowerCase())) {
                throw new ConfigException(key, value, format("Invalid enumerator value. Should be one of: %s", values));
            }
        }

        @Override
        public String toString() {
            return values.toString();
        }

        @Override
        public List<Object> validValues(final String name, final Map<String, Object> parsedConfig) {
            return new ArrayList<>(values);
        }

        @Override
        public boolean visible(final String name, final Map<String, Object> parsedConfig) {
            return true;
        }
    }

    private Validators() {
    }
}
