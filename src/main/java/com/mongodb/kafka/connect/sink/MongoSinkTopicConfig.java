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

package com.mongodb.kafka.connect.sink;

import static com.mongodb.kafka.connect.sink.MongoSinkConfig.CONNECTION_URI_CONFIG;
import static com.mongodb.kafka.connect.sink.MongoSinkConfig.TOPICS_CONFIG;
import static com.mongodb.kafka.connect.util.ClassHelper.createInstance;
import static com.mongodb.kafka.connect.util.Validators.errorCheckingValueValidator;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import com.mongodb.MongoNamespace;

import com.mongodb.kafka.connect.sink.cdc.CdcHandler;
import com.mongodb.kafka.connect.sink.processor.BlacklistKeyProjector;
import com.mongodb.kafka.connect.sink.processor.BlacklistValueProjector;
import com.mongodb.kafka.connect.sink.processor.PostProcessors;
import com.mongodb.kafka.connect.sink.processor.WhitelistKeyProjector;
import com.mongodb.kafka.connect.sink.processor.WhitelistValueProjector;
import com.mongodb.kafka.connect.sink.processor.field.projection.FieldProjector;
import com.mongodb.kafka.connect.sink.processor.id.strategy.FullKeyStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.IdStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialKeyStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.PartialValueStrategy;
import com.mongodb.kafka.connect.sink.processor.id.strategy.ProvidedInKeyStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.DeleteOneDefaultStrategy;
import com.mongodb.kafka.connect.sink.writemodel.strategy.WriteModelStrategy;
import com.mongodb.kafka.connect.util.ConfigHelper;
import com.mongodb.kafka.connect.util.ConnectConfigException;
import com.mongodb.kafka.connect.util.Validators;

public class MongoSinkTopicConfig extends AbstractConfig {

    public enum FieldProjectionType {
        NONE,
        BLACKLIST,
        WHITELIST
    }

    private static final String TOPIC_CONFIG = "topic";
    static final String TOPIC_OVERRIDE_PREFIX = "topic.override.";

    public static final String DATABASE_CONFIG = "database";
    private static final String DATABASE_DISPLAY = "The MongoDB database name.";
    private static final String DATABASE_DOC = "The database for the sink to write.";

    public static final String COLLECTION_CONFIG = "collection";
    private static final String COLLECTION_DISPLAY = "The default MongoDB collection name";
    private static final String COLLECTION_DOC = "Optional, single sink collection name to write to. If following multiple topics then "
            + "this will be the default collection they are mapped to.";
    private static final String COLLECTION_DEFAULT = "";

    public static final String MAX_NUM_RETRIES_CONFIG = "max.num.retries";
    private static final String MAX_NUM_RETRIES_DISPLAY = "Max number of retries";
    private static final String MAX_NUM_RETRIES_DOC = "How often a retry should be done on write errors";
    private static final int MAX_NUM_RETRIES_DEFAULT = 3;

    public static final String RETRIES_DEFER_TIMEOUT_CONFIG = "retries.defer.timeout";
    private static final String RETRIES_DEFER_TIMEOUT_DISPLAY = "Retry defer timeout";
    private static final String RETRIES_DEFER_TIMEOUT_DOC = "How long in ms a retry should get deferred";
    private static final int RETRIES_DEFER_TIMEOUT_DEFAULT = 5000;

    public static final String DOCUMENT_ID_STRATEGY_CONFIG = "document.id.strategy";
    private static final String DOCUMENT_ID_STRATEGY_DISPLAY = "The document id strategy";
    private static final String DOCUMENT_ID_STRATEGY_DOC = "The IdStrategy class name to use for generating a unique document id (_id)";
    private static final String DOCUMENT_ID_STRATEGY_DEFAULT = "com.mongodb.kafka.connect.sink.processor.id.strategy.BsonOidStrategy";

    public static final String DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG = "document.id.strategy.overwrite.existing";
    private static final String DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DISPLAY = "The document id strategy overwrite existing setting";
    private static final String DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DOC = "Allows the document id strategy will overwrite existing `_id` values";
    private static final boolean DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DEFAULT = false;

    public static final String KEY_PROJECTION_TYPE_CONFIG = "key.projection.type";
    private static final String KEY_PROJECTION_TYPE_DISPLAY = "The key projection type";
    private static final String KEY_PROJECTION_TYPE_DOC = "The type of key projection to use";
    private static final String KEY_PROJECTION_TYPE_DEFAULT = "none";

    public static final String KEY_PROJECTION_LIST_CONFIG = "key.projection.list";
    private static final String KEY_PROJECTION_LIST_DISPLAY = "The key projection list";
    private static final String KEY_PROJECTION_LIST_DOC = "A comma separated list of field names for key projection";
    private static final String KEY_PROJECTION_LIST_DEFAULT = "";

    public static final String VALUE_PROJECTION_TYPE_CONFIG = "value.projection.type";
    private static final String VALUE_PROJECTION_TYPE_DISPLAY = "The type of value projection to use";
    private static final String VALUE_PROJECTION_TYPE_DOC = "The type of value projection to use";
    private static final String VALUE_PROJECTION_TYPE_DEFAULT = "none";

    public static final String VALUE_PROJECTION_LIST_CONFIG = "value.projection.list";
    private static final String VALUE_PROJECTION_LIST_DISPLAY = "The value projection list";
    private static final String VALUE_PROJECTION_LIST_DOC = "A comma separated list of field names for value projection";
    private static final String VALUE_PROJECTION_LIST_DEFAULT = "";

    public static final String FIELD_RENAMER_MAPPING_CONFIG = "field.renamer.mapping";
    private static final String FIELD_RENAMER_MAPPING_DISPLAY = "The field renamer mapping";
    private static final String FIELD_RENAMER_MAPPING_DOC = "An inline JSON array with objects describing field name mappings.\n"
            + "Example: `[{\"oldName\":\"key.fieldA\",\"newName\":\"field1\"},{\"oldName\":\"value.xyz\",\"newName\":\"abc\"}]`";
    private static final String FIELD_RENAMER_MAPPING_DEFAULT = "[]";

    public static final String FIELD_RENAMER_REGEXP_CONFIG = "field.renamer.regexp";
    public static final String FIELD_RENAMER_REGEXP_DISPLAY = "The field renamer regex";
    private static final String FIELD_RENAMER_REGEXP_DOC = "An inline JSON array with objects describing regexp settings.\n"
            + "Example: `[[{\"regexp\":\"^key\\\\\\\\..*my.*$\",\"pattern\":\"my\",\"replace\":\"\"},"
            + "{\"regexp\":\"^value\\\\\\\\..*$\",\"pattern\":\"\\\\\\\\.\",\"replace\":\"_\"}]`";
    private static final String FIELD_RENAMER_REGEXP_DEFAULT = "[]";

    public static final String POST_PROCESSOR_CHAIN_CONFIG = "post.processor.chain";
    private static final String POST_PROCESSOR_CHAIN_DISPLAY = "The post processor chain";
    private static final String POST_PROCESSOR_CHAIN_DOC = "A comma separated list of post processor classes to process the data before "
            + "saving to MongoDB.";
    private static final String POST_PROCESSOR_CHAIN_DEFAULT = "com.mongodb.kafka.connect.sink.processor.DocumentIdAdder";

    public static final String CHANGE_DATA_CAPTURE_HANDLER_CONFIG = "change.data.capture.handler";
    private static final String CHANGE_DATA_CAPTURE_HANDLER_DISPLAY = "The CDC handler";
    private static final String CHANGE_DATA_CAPTURE_HANDLER_DOC = "The class name of the CDC handler to use for processing";
    private static final String CHANGE_DATA_CAPTURE_HANDLER_DEFAULT = "";

    public static final String DELETE_ON_NULL_VALUES_CONFIG = "delete.on.null.values";
    private static final String DELETE_ON_NULL_VALUES_DISPLAY = "Delete on null values";
    private static final String DELETE_ON_NULL_VALUES_DOC = "Whether or not the connector tries to delete documents based on key when "
            + "value is null";
    private static final boolean DELETE_ON_NULL_VALUES_DEFAULT = false;

    public static final String WRITEMODEL_STRATEGY_CONFIG = "writemodel.strategy";
    private static final String WRITEMODEL_STRATEGY_DISPLAY = "The writeModel strategy";
    private static final String WRITEMODEL_STRATEGY_DOC = "The class the handles how build the write models for the sink documents";
    private static final String WRITEMODEL_STRATEGY_DEFAULT =
            "com.mongodb.kafka.connect.sink.writemodel.strategy.ReplaceOneDefaultStrategy";

    public static final String MAX_BATCH_SIZE_CONFIG = "max.batch.size";
    private static final String MAX_BATCH_SIZE_DISPLAY = "The maximum batch size";
    private static final String MAX_BATCH_SIZE_DOC = "The maximum number of sink records to possibly batch together for processing";
    private static final int MAX_BATCH_SIZE_DEFAULT = 0;

    public static final String RATE_LIMITING_TIMEOUT_CONFIG = "rate.limiting.timeout";
    private static final String RATE_LIMITING_TIMEOUT_DISPLAY = "The rate limiting timeout";
    private static final String RATE_LIMITING_TIMEOUT_DOC = "How long in ms processing should wait before continue processing";
    private static final int RATE_LIMITING_TIMEOUT_DEFAULT = 0;

    public static final String RATE_LIMITING_EVERY_N_CONFIG = "rate.limiting.every.n";
    private static final String RATE_LIMITING_EVERY_N_DISPLAY = "The rate limiting batch number";
    private static final String RATE_LIMITING_EVERY_N_DOC = "After how many processed batches the rate limit should trigger "
            + "(NO rate limiting if n=0)";
    private static final int RATE_LIMITING_EVERY_N_DEFAULT = 0;

    private static final Pattern CLASS_NAME = Pattern.compile("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
    private static final Pattern FULLY_QUALIFIED_CLASS_NAME = Pattern.compile("(" + CLASS_NAME + "\\.)*" + CLASS_NAME);

    public static final String ID_FIELD = "_id";

    private static final List<Consumer<MongoSinkTopicConfig>> INITIALIZERS = asList(
            MongoSinkTopicConfig::getNamespace, MongoSinkTopicConfig::getIdStrategy, MongoSinkTopicConfig::getPostProcessors,
            MongoSinkTopicConfig::getWriteModelStrategy, MongoSinkTopicConfig::getDeleteOneWriteModelStrategy,
            MongoSinkTopicConfig::getRateLimitSettings, MongoSinkTopicConfig::getCdcHandler);

    private final String topic;
    private MongoNamespace namespace;
    private IdStrategy idStrategy;
    private PostProcessors postProcessors;
    private WriteModelStrategy writeModelStrategy;
    private WriteModelStrategy deleteOneWriteModelStrategy;
    private RateLimitSettings rateLimitSettings;
    private CdcHandler cdcHandler;

    MongoSinkTopicConfig(final String topic, final Map<String, String> originals) {
        this(topic, originals, true);
    }

    private MongoSinkTopicConfig(final String topic, final Map<String, String> originals, final boolean initializeAll) {
        super(CONFIG, createSinkTopicOriginals(topic, originals));
        this.topic = topic;

        if (initializeAll) {
            INITIALIZERS.forEach(i -> i.accept(this));
        }
    }

    static final ConfigDef BASE_CONFIG = createConfigDef();

    static final ConfigDef CONFIG = createConfigDef()
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Topic name");

    public String getTopic() {
        return topic;
    }

    public MongoNamespace getNamespace() {
        if (namespace == null) {
            String database = getString(DATABASE_CONFIG);
            if (database.isEmpty()) {
                throw new ConnectConfigException(DATABASE_CONFIG, database, "Missing database name for configuration.");
            }
            String collection = getString(COLLECTION_CONFIG);
            if (collection.isEmpty()) {
                collection = topic;
            }
            namespace = new MongoNamespace(database, collection);
        }
        return namespace;
    }

    @SuppressWarnings("unchecked")
    public IdStrategy getIdStrategy() {
        if (idStrategy == null) {
            String className = getString(DOCUMENT_ID_STRATEGY_CONFIG);
            Optional<FieldProjector> fieldProjector = getFieldProjector(className);
            idStrategy = createInstance(DOCUMENT_ID_STRATEGY_CONFIG, className, IdStrategy.class, () -> {
                Class<IdStrategy> clazz = (Class<IdStrategy>) Class.forName(className);
                boolean hasFieldProjectorConstructor = fieldProjector.isPresent()
                        && Arrays.stream(clazz.getConstructors()).anyMatch(i -> i.getParameterTypes().length == 1
                        && i.getParameterTypes()[0].equals(FieldProjector.class));
                if (hasFieldProjectorConstructor) {
                    return clazz.getConstructor(FieldProjector.class).newInstance(fieldProjector.get());
                }
                return clazz.getConstructor().newInstance();
            });
        }
        return idStrategy;
    }

    PostProcessors getPostProcessors() {
        if (postProcessors == null) {
            postProcessors = new PostProcessors(this, getList(POST_PROCESSOR_CHAIN_CONFIG));
        }
        return postProcessors;
    }

    WriteModelStrategy getWriteModelStrategy() {
        if (writeModelStrategy == null) {
            writeModelStrategy = createInstance(WRITEMODEL_STRATEGY_CONFIG, getString(WRITEMODEL_STRATEGY_CONFIG),
                    WriteModelStrategy.class);
        }
        return writeModelStrategy;
    }

    Optional<WriteModelStrategy> getDeleteOneWriteModelStrategy() {
        if (!getBoolean(DELETE_ON_NULL_VALUES_CONFIG)) {
            return Optional.empty();
        }
        if (deleteOneWriteModelStrategy == null) {
                /*
                NOTE: DeleteOneModel requires the key document which means that the only reasonable ID generation strategies are those
                which refer to/operate on the key document. Thus currently this means the IdStrategy must be either:

                FullKeyStrategy
                PartialKeyStrategy
                ProvidedInKeyStrategy
                */
            IdStrategy idStrategy = getIdStrategy();
            if (!(idStrategy instanceof FullKeyStrategy) && !(idStrategy instanceof PartialKeyStrategy)
                    && !(idStrategy instanceof ProvidedInKeyStrategy)) {
                throw new ConnectConfigException(DELETE_ON_NULL_VALUES_CONFIG, getBoolean(DELETE_ON_NULL_VALUES_CONFIG),
                        format("Error: %s can only be applied when the configured IdStrategy is an instance of: %s or %s or %s",
                                DeleteOneDefaultStrategy.class.getSimpleName(), FullKeyStrategy.class.getSimpleName(),
                                PartialKeyStrategy.class.getSimpleName(), ProvidedInKeyStrategy.class.getSimpleName()));
            }
            deleteOneWriteModelStrategy = new DeleteOneDefaultStrategy(idStrategy);
        }
        return Optional.of(deleteOneWriteModelStrategy);
    }

    Optional<CdcHandler> getCdcHandler() {
        String cdcHandler = getString(CHANGE_DATA_CAPTURE_HANDLER_CONFIG);
        if (cdcHandler.isEmpty()) {
            return Optional.empty();
        }

        if (this.cdcHandler == null) {
            this.cdcHandler = createInstance(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, cdcHandler, CdcHandler.class,
                    () -> (CdcHandler) Class.forName(cdcHandler).getConstructor(MongoSinkTopicConfig.class).newInstance(this));
        }
        return Optional.of(this.cdcHandler);
    }

    RateLimitSettings getRateLimitSettings() {
        if (rateLimitSettings == null) {
            rateLimitSettings = new RateLimitSettings(getInt(RATE_LIMITING_TIMEOUT_CONFIG), getInt(RATE_LIMITING_EVERY_N_CONFIG));
        }
        return rateLimitSettings;
    }

    static Map<String, ConfigValue> validateAll(final String topic, final Map<String, String> props) {
        String prefix = format("%s%s.", TOPIC_OVERRIDE_PREFIX, topic);
        List<String> topicOverrides = props.keySet().stream().filter(k -> k.startsWith(prefix))
                .map(k -> k.substring(0, prefix.length()))
                .collect(Collectors.toList());

        Map<String, ConfigValue> results = new HashMap<>();
        AtomicBoolean containsError = new AtomicBoolean();
        Map<String, String> sinkTopicOriginals = createSinkTopicOriginals(topic, props);

        CONFIG.validateAll(sinkTopicOriginals).forEach((k, v) -> {
            if (!v.errorMessages().isEmpty()) {
                containsError.set(true);
            }

            String name = topicOverrides.contains(k) ? prefix + k : k;
            results.put(name, new ConfigValue(name, v.value(), v.recommendedValues(), v.errorMessages()));
        });

        if (!containsError.get()) {
            MongoSinkTopicConfig cfg = new MongoSinkTopicConfig(topic, sinkTopicOriginals, false);
            INITIALIZERS.forEach(i -> {
                try {
                    i.accept(cfg);
                } catch (ConnectConfigException t) {
                    results.put(t.getName(), new ConfigValue(t.getName(), t.getValue(), emptyList(), singletonList(t.getMessage())));
                }
            });
        }

        return results;
    }

    private static Map<String, String> createSinkTopicOriginals(final String topic, final Map<String, String> originals) {
        Map<String, String> topicConfig = new HashMap<>();
        Map<String, String> topicOverrides = new HashMap<>();
        String topicOverridePrefix = format("%s%s", TOPIC_OVERRIDE_PREFIX, topic);
        topicConfig.put(TOPIC_CONFIG, topic);
        originals.forEach((k, v) -> {
            if (!k.startsWith(TOPIC_OVERRIDE_PREFIX) && !k.equals(CONNECTION_URI_CONFIG) && !k.equals(TOPICS_CONFIG)) {
                topicConfig.put(k, v);
            }
            if (k.startsWith(topicOverridePrefix)) {
                topicOverrides.put(k.substring(topicOverridePrefix.length() + 1), v);
            }
        });

        topicConfig.putAll(topicOverrides);
        return topicConfig;
    }

    static Map<String, ConfigValue> validateRegexAll(final Map<String, String> props) {
        Map<String, ConfigValue> results = new HashMap<>();
        AtomicBoolean containsError = new AtomicBoolean();
        Map<String, String> sinkTopicOriginals = createSinkTopicOriginals(props);

        CONFIG.validateAll(sinkTopicOriginals).forEach((k, v) -> {
            if (!v.errorMessages().isEmpty()) {
                containsError.set(true);
            }
            results.put(k, new ConfigValue(k, v.value(), v.recommendedValues(), v.errorMessages()));
        });

        props.keySet().stream().filter(k -> k.startsWith(TOPIC_OVERRIDE_PREFIX))
                .map(k -> k.substring(TOPIC_OVERRIDE_PREFIX.length()).split("\\.")[0])
                .forEach(t -> results.putAll(validateAll(t, props)));

        return results;
    }

    private static Map<String, String> createSinkTopicOriginals(final Map<String, String> originals) {
        Map<String, String> topicConfig = new HashMap<>();
        originals.forEach((k, v) -> {
            if (!k.startsWith(TOPIC_OVERRIDE_PREFIX) && !k.equals(CONNECTION_URI_CONFIG) && !k.equals(TOPICS_CONFIG)) {
                topicConfig.put(k, v);
            }
        });
        return topicConfig;
    }

    private Optional<FieldProjector> getFieldProjector(final String strategyClassName) {
        FieldProjectionType keyProjectionType = FieldProjectionType.valueOf(getString(KEY_PROJECTION_TYPE_CONFIG).toUpperCase());
        FieldProjectionType valueProjectionType = FieldProjectionType.valueOf(getString(VALUE_PROJECTION_TYPE_CONFIG).toUpperCase());

        if (keyProjectionType.equals(FieldProjectionType.NONE) && strategyClassName.equals(PartialKeyStrategy.class.getName())) {
            throw new ConnectConfigException(DOCUMENT_ID_STRATEGY_CONFIG, strategyClassName,
                    format("Invalid %s value", KEY_PROJECTION_TYPE_CONFIG));
        } else if (valueProjectionType.equals(FieldProjectionType.NONE) && strategyClassName.equals(PartialValueStrategy.class.getName())) {
            throw new ConnectConfigException(DOCUMENT_ID_STRATEGY_CONFIG, strategyClassName,
                    format("Invalid %s value", VALUE_PROJECTION_TYPE_CONFIG));
        }

        if (keyProjectionType.equals(FieldProjectionType.BLACKLIST)) {
            return Optional.of(new BlacklistKeyProjector(this));
        } else if (keyProjectionType.equals(FieldProjectionType.WHITELIST)) {
            return Optional.of(new WhitelistKeyProjector(this));
        }

        if (valueProjectionType.equals(FieldProjectionType.BLACKLIST)) {
            return Optional.of(new BlacklistValueProjector(this));
        } else if (valueProjectionType.equals(FieldProjectionType.WHITELIST)) {
            return Optional.of(new WhitelistValueProjector(this));
        }

        return Optional.empty();
    }

    private static ConfigDef createConfigDef() {

        ConfigDef configDef = new ConfigDef();

        String group = "Namespace";
        int orderInGroup = 0;
        configDef.define(DATABASE_CONFIG,
                ConfigDef.Type.STRING,
                NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                DATABASE_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                DATABASE_DISPLAY);
        configDef.define(COLLECTION_CONFIG,
                ConfigDef.Type.STRING,
                COLLECTION_DEFAULT,
                ConfigDef.Importance.HIGH,
                COLLECTION_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                COLLECTION_DISPLAY);

        group = "Writes";
        orderInGroup = 0;
        configDef.define(MAX_NUM_RETRIES_CONFIG,
                ConfigDef.Type.INT,
                MAX_NUM_RETRIES_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_NUM_RETRIES_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                MAX_NUM_RETRIES_DISPLAY);
        configDef.define(RETRIES_DEFER_TIMEOUT_CONFIG,
                ConfigDef.Type.INT,
                RETRIES_DEFER_TIMEOUT_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.MEDIUM,
                RETRIES_DEFER_TIMEOUT_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                RETRIES_DEFER_TIMEOUT_DISPLAY);
        configDef.define(DELETE_ON_NULL_VALUES_CONFIG,
                ConfigDef.Type.BOOLEAN,
                DELETE_ON_NULL_VALUES_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                DELETE_ON_NULL_VALUES_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                DELETE_ON_NULL_VALUES_DISPLAY);
        configDef.define(WRITEMODEL_STRATEGY_CONFIG,
                ConfigDef.Type.STRING,
                WRITEMODEL_STRATEGY_DEFAULT,
                Validators.matching(FULLY_QUALIFIED_CLASS_NAME),
                ConfigDef.Importance.LOW,
                WRITEMODEL_STRATEGY_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                WRITEMODEL_STRATEGY_DISPLAY);
        configDef.define(MAX_BATCH_SIZE_CONFIG,
                ConfigDef.Type.INT,
                MAX_BATCH_SIZE_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.MEDIUM,
                MAX_BATCH_SIZE_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                MAX_BATCH_SIZE_DISPLAY);
        configDef.define(RATE_LIMITING_TIMEOUT_CONFIG,
                ConfigDef.Type.INT,
                RATE_LIMITING_TIMEOUT_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                RATE_LIMITING_TIMEOUT_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                RATE_LIMITING_TIMEOUT_DISPLAY);
        configDef.define(RATE_LIMITING_EVERY_N_CONFIG,
                ConfigDef.Type.INT,
                RATE_LIMITING_EVERY_N_DEFAULT,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                RATE_LIMITING_EVERY_N_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                RATE_LIMITING_EVERY_N_DISPLAY);

        group = "Post Processing";
        orderInGroup = 0;
        configDef.define(POST_PROCESSOR_CHAIN_CONFIG,
                ConfigDef.Type.LIST,
                POST_PROCESSOR_CHAIN_DEFAULT,
                Validators.listMatchingPattern(FULLY_QUALIFIED_CLASS_NAME),
                ConfigDef.Importance.LOW,
                POST_PROCESSOR_CHAIN_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                POST_PROCESSOR_CHAIN_DISPLAY);
        configDef.define(KEY_PROJECTION_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                KEY_PROJECTION_TYPE_DEFAULT,
                Validators.EnumValidatorAndRecommender.in(FieldProjectionType.values()),
                ConfigDef.Importance.LOW,
                KEY_PROJECTION_TYPE_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                KEY_PROJECTION_TYPE_DISPLAY,
                Validators.EnumValidatorAndRecommender.in(FieldProjectionType.values()));
        configDef.define(KEY_PROJECTION_LIST_CONFIG,
                ConfigDef.Type.STRING,
                KEY_PROJECTION_LIST_DEFAULT,
                ConfigDef.Importance.LOW,
                KEY_PROJECTION_LIST_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                KEY_PROJECTION_LIST_DISPLAY,
                singletonList(KEY_PROJECTION_TYPE_CONFIG));
        configDef.define(VALUE_PROJECTION_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                VALUE_PROJECTION_TYPE_DEFAULT,
                Validators.EnumValidatorAndRecommender.in(FieldProjectionType.values()),
                ConfigDef.Importance.LOW,
                VALUE_PROJECTION_TYPE_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                VALUE_PROJECTION_TYPE_DISPLAY,
                Validators.EnumValidatorAndRecommender.in(FieldProjectionType.values()));
        configDef.define(VALUE_PROJECTION_LIST_CONFIG,
                ConfigDef.Type.STRING,
                VALUE_PROJECTION_LIST_DEFAULT,
                ConfigDef.Importance.LOW,
                VALUE_PROJECTION_LIST_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                VALUE_PROJECTION_LIST_DISPLAY,
                singletonList(VALUE_PROJECTION_TYPE_CONFIG));
        configDef.define(FIELD_RENAMER_MAPPING_CONFIG,
                ConfigDef.Type.STRING,
                FIELD_RENAMER_MAPPING_DEFAULT,
                errorCheckingValueValidator("A valid JSON array", ConfigHelper::jsonArrayFromString),
                ConfigDef.Importance.LOW,
                FIELD_RENAMER_MAPPING_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                FIELD_RENAMER_MAPPING_DISPLAY);
        configDef.define(FIELD_RENAMER_REGEXP_CONFIG,
                ConfigDef.Type.STRING,
                FIELD_RENAMER_REGEXP_DEFAULT,
                errorCheckingValueValidator("A valid JSON array", ConfigHelper::jsonArrayFromString),
                ConfigDef.Importance.LOW,
                FIELD_RENAMER_REGEXP_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                FIELD_RENAMER_REGEXP_DISPLAY);

        group = "Id Strategies";
        orderInGroup = 0;
        // Id strategies
        configDef.define(DOCUMENT_ID_STRATEGY_CONFIG,
                ConfigDef.Type.STRING,
                DOCUMENT_ID_STRATEGY_DEFAULT,
                Validators.emptyString().or(Validators.matching(FULLY_QUALIFIED_CLASS_NAME)),
                ConfigDef.Importance.HIGH,
                DOCUMENT_ID_STRATEGY_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                DOCUMENT_ID_STRATEGY_DISPLAY);

        configDef.define(DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_CONFIG,
                ConfigDef.Type.BOOLEAN,
                DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DEFAULT,
                ConfigDef.Importance.HIGH,
                DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                DOCUMENT_ID_STRATEGY_OVERWRITE_EXISTING_DISPLAY);

        group = "Change Data Capture";
        orderInGroup = 0;
        configDef.define(CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                ConfigDef.Type.STRING,
                CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                Validators.emptyString().or(Validators.matching(FULLY_QUALIFIED_CLASS_NAME)),
                ConfigDef.Importance.LOW,
                CHANGE_DATA_CAPTURE_HANDLER_DOC,
                group,
                ++orderInGroup,
                ConfigDef.Width.MEDIUM,
                CHANGE_DATA_CAPTURE_HANDLER_DISPLAY);
        return configDef;
    }
}
