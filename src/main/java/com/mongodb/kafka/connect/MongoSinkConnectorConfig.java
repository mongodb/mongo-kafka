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

package com.mongodb.kafka.connect;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.apache.kafka.common.config.ConfigDef.Width;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;

import org.bson.Document;
import org.bson.json.JsonParseException;

import com.mongodb.ConnectionString;

import com.mongodb.kafka.connect.cdc.CdcHandler;
import com.mongodb.kafka.connect.cdc.debezium.mongodb.MongoDbHandler;
import com.mongodb.kafka.connect.cdc.debezium.rdbms.RdbmsHandler;
import com.mongodb.kafka.connect.cdc.debezium.rdbms.mysql.MysqlHandler;
import com.mongodb.kafka.connect.cdc.debezium.rdbms.postgres.PostgresHandler;
import com.mongodb.kafka.connect.processor.BlacklistKeyProjector;
import com.mongodb.kafka.connect.processor.BlacklistValueProjector;
import com.mongodb.kafka.connect.processor.DocumentIdAdder;
import com.mongodb.kafka.connect.processor.PostProcessor;
import com.mongodb.kafka.connect.processor.WhitelistKeyProjector;
import com.mongodb.kafka.connect.processor.WhitelistValueProjector;
import com.mongodb.kafka.connect.processor.field.projection.FieldProjector;
import com.mongodb.kafka.connect.processor.field.renaming.RegExpSettings;
import com.mongodb.kafka.connect.processor.id.strategy.BsonOidStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.FullKeyStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.IdStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.KafkaMetaDataStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.PartialKeyStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.PartialValueStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.ProvidedInKeyStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.ProvidedInValueStrategy;
import com.mongodb.kafka.connect.processor.id.strategy.UuidStrategy;
import com.mongodb.kafka.connect.writemodel.strategy.DeleteOneDefaultStrategy;
import com.mongodb.kafka.connect.writemodel.strategy.WriteModelStrategy;

public class MongoSinkConnectorConfig extends CollectionAwareConfig {

    public enum FieldProjectionTypes {
        NONE,
        BLACKLIST,
        WHITELIST
    }



    public static final String CONNECTION_URI_CONFIG = "mongodb.connection.uri";
    private static final String CONNECTION_URI_DISPLAY = "MongoDB Connection URI";
    private static final String CONNECTION_URI_DOC = "The mongodb connection URI as supported by the official drivers. "
            + "eg: mongodb://user@pass@locahost/";

    public static final String DATABASE_NAME_CONFIG = "mongodb.database";
    private static final String DATABASE_NAME_DISPLAY = "The MongoDB database name.";
    private static final String DATABASE_NAME_DOC = "The database for the sink to write. Overwrites the database from the connection uri.";

    public static final String COLLECTION_CONFIG = "mongodb.collection";
    private static final String COLLECTION_DISPLAY = "The MongoDB collection name";
    private static final String COLLECTION_DOC = "Single sink collection name to write to";

    public static final String COLLECTIONS_CONFIG = "mongodb.collections";
    private static final String COLLECTIONS_DISPLAY = "MongoDB collection names";
    private static final String COLLECTIONS_DOC = "Names of sink collections to write to for which there can be topic-level specific "
            + "properties defined";

    public static final String MAX_NUM_RETRIES_CONFIG = "mongodb.max.num.retries";
    private static final String MAX_NUM_RETRIES_DOC = "how often a retry should be done on write errors";

    public static final String RETRIES_DEFER_TIMEOUT_CONFIG = "mongodb.retries.defer.timeout";
    private static final String RETRIES_DEFER_TIMEOUT_DOC = "how long in ms a retry should get deferred";

    public static final String VALUE_PROJECTION_TYPE_CONFIG = "mongodb.value.projection.type";
    private static final String VALUE_PROJECTION_TYPE_DOC = "whether or not and which value projection to use";

    public static final String VALUE_PROJECTION_LIST_CONFIG = "mongodb.value.projection.list";
    private static final String VALUE_PROJECTION_LIST_DOC = "comma separated list of field names for value projection";

    public static final String DOCUMENT_ID_STRATEGY_CONFIG = "mongodb.document.id.strategy";
    private static final String DOCUMENT_ID_STRATEGY_CONF_DOC = "class name of strategy to use for generating a unique document id (_id)";

    public static final String DOCUMENT_ID_STRATEGIES_CONFIG = "mongodb.document.id.strategies";
    private static final String DOCUMENT_ID_STRATEGIES_CONF_DOC = "comma separated list of custom strategy classes to register for usage";

    public static final String KEY_PROJECTION_TYPE_CONFIG = "mongodb.key.projection.type";
    private static final String KEY_PROJECTION_TYPE_DOC = "whether or not and which key projection to use";

    public static final String KEY_PROJECTION_LIST_CONFIG = "mongodb.key.projection.list";
    private static final String KEY_PROJECTION_LIST_DOC = "comma separated list of field names for key projection";

    public static final String FIELD_RENAMER_MAPPING_CONFIG = "mongodb.field.renamer.mapping";
    private static final String FIELD_RENAMER_MAPPING_DOC = "inline JSON array with objects describing field name mappings";

    public static final String FIELD_RENAMER_REGEXP_CONFIG = "mongodb.field.renamer.regexp";
    private static final String FIELD_RENAMER_REGEXP_DOC = "inline JSON array with objects describing regexp settings";

    public static final String POST_PROCESSOR_CHAIN_CONFIG = "mongodb.post.processor.chain";
    private static final String POST_PROCESSOR_CHAIN_DOC = "comma separated list of post processor classes to build the chain with";

    public static final String CHANGE_DATA_CAPTURE_HANDLER_CONFIG = "mongodb.change.data.capture.handler";
    private static final String CHANGE_DATA_CAPTURE_HANDLER_DOC = "class name of CDC handler to use for processing";

    public static final String DELETE_ON_NULL_VALUES_CONFIG = "mongodb.delete.on.null.values";
    private static final String DELETE_ON_NULL_VALUES_DOC = "whether or not the connector tries to delete documents based on key when "
            + "value is null";

    public static final String WRITEMODEL_STRATEGY_CONFIG = "mongodb.writemodel.strategy";
    private static final String WRITEMODEL_STRATEGY_DOC = "how to build the write models for the sink documents";

    public static final String MAX_BATCH_SIZE_CONFIG = "mongodb.max.batch.size";
    private static final String MAX_BATCH_SIZE_DOC = "maximum number of sink records to possibly batch together for processing";

    public static final String RATE_LIMITING_TIMEOUT_CONFIG = "mongodb.rate.limiting.timeout";
    private static final String RATE_LIMITING_TIMEOUT_DOC = "how long in ms processing should wait before continue processing";

    public static final String RATE_LIMITING_EVERY_N_CONFIG = "mongodb.rate.limiting.every.n";
    private static final String RATE_LIMITING_EVERY_N_DOC = "after how many processed batches the rate limit should trigger "
            + "(NO rate limiting if n=0)";

    private static final Pattern CLASS_NAME = Pattern.compile("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
    private static final Pattern FULLY_QUALIFIED_CLASS_NAME = Pattern.compile("(" + CLASS_NAME + "\\.)*" + CLASS_NAME);
    private static final Pattern FULLY_QUALIFIED_CLASS_NAME_LIST =
            Pattern.compile("(" + FULLY_QUALIFIED_CLASS_NAME + ",)*" + FULLY_QUALIFIED_CLASS_NAME);

    static final String FIELD_LIST_SPLIT_CHAR = ",";
    private static final String FIELD_LIST_SPLIT_EXPR = "\\s*" + FIELD_LIST_SPLIT_CHAR + "\\s*";

    static final String TOPIC_AGNOSTIC_KEY_NAME = "__default__";

    public static final String ID_FIELD = "_id";
    private static final String CONNECTION_URI_DEFAULT = "mongodb://localhost:27017";
    private static final String DATABASE_DEFAULT = "";
    private static final String COLLECTIONS_DEFAULT = "";
    private static final String COLLECTION_DEFAULT = "";
    private static final int MAX_NUM_RETRIES_DEFAULT = 3;
    private static final int RETRIES_DEFER_TIMEOUT_DEFAULT = 5000;
    private static final String VALUE_PROJECTION_TYPE_DEFAULT = "none";
    private static final String VALUE_PROJECTION_LIST_DEFAULT = "";
    private static final String DOCUMENT_ID_STRATEGY_DEFAULT = "com.mongodb.kafka.connect.processor.id.strategy.BsonOidStrategy";
    private static final String DOCUMENT_ID_STRATEGIES_DEFAULT = "";
    private static final String KEY_PROJECTION_TYPE_DEFAULT = "none";
    private static final String KEY_PROJECTION_LIST_DEFAULT = "";
    private static final String FIELD_RENAMER_MAPPING_DEFAULT = "[]";
    private static final String FIELD_RENAMER_REGEXP_DEFAULT = "[]";
    private static final String POST_PROCESSOR_CHAIN_DEFAULT = "com.mongodb.kafka.connect.processor.DocumentIdAdder";
    private static final String CHANGE_DATA_CAPTURE_HANDLER_DEFAULT = "";
    private static final boolean DELETE_ON_NULL_VALUES_DEFAULT = false;
    private static final String WRITEMODEL_STRATEGY_DEFAULT = "com.mongodb.kafka.connect.writemodel.strategy.ReplaceOneDefaultStrategy";
    private static final int MAX_BATCH_SIZE_DEFAULT = 0;
    private static final int RATE_LIMITING_TIMEOUT_DEFAULT = 0;
    private static final int RATE_LIMITING_EVERY_N_DEFAULT = 0;

    static class RateLimitSettings {

        private final int timeoutMs;
        private final int everyN;
        private long counter;

        RateLimitSettings(final int timeoutMs, final int everyN) {
            this.timeoutMs = timeoutMs;
            this.everyN = everyN;
        }

        boolean isTriggered() {
            counter++;
            return (everyN != 0) && (counter >= everyN) && (counter % everyN == 0);
        }

        int getTimeoutMs() {
            return timeoutMs;
        }

        int getEveryN() {
            return everyN;
        }
    }

    public MongoSinkConnectorConfig(final ConfigDef config, final Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MongoSinkConnectorConfig(final Map<String, String> parsedConfig) {
        this(CONFIG, parsedConfig);
    }

    public boolean isUsingBlacklistValueProjection(final String collection) {
        return getString(VALUE_PROJECTION_TYPE_CONFIG, collection).equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name());
    }

    public boolean isUsingWhitelistValueProjection(final String collection) {
        return getString(VALUE_PROJECTION_TYPE_CONFIG, collection).equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name());
    }

    public boolean isUsingBlacklistKeyProjection(final String collection) {
        return getString(KEY_PROJECTION_TYPE_CONFIG, collection).equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name());
    }

    public boolean isUsingWhitelistKeyProjection(final String collection) {
        return getString(KEY_PROJECTION_TYPE_CONFIG, collection).equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name());
    }

    public Set<String> getKeyProjectionList(final String collection) {
        return buildProjectionList(getString(KEY_PROJECTION_TYPE_CONFIG, collection),
                getString(KEY_PROJECTION_LIST_CONFIG, collection));
    }

    public Set<String> getValueProjectionList(final String collection) {
        return buildProjectionList(getString(VALUE_PROJECTION_TYPE_CONFIG, collection),
                getString(VALUE_PROJECTION_LIST_CONFIG, collection));
    }

    public Map<String, String> parseRenameFieldnameMappings(final String collection) {
        String settings = getString(FIELD_RENAMER_MAPPING_CONFIG, collection);
        if (settings.isEmpty()) {
            return new HashMap<>();
        }
        try {
            Document allSettings = Document.parse(format("{settings: %s}", settings));
            List<Document> settingsList = allSettings.get("settings", emptyList());
            Map<String, String> map = new HashMap<>();
            for (Document e : settingsList) {
                if (!(e.containsKey("oldName") || e.containsKey("newName"))) {
                    throw new ConfigException(format("Error: parsing rename fieldname mappings failed: %s", settings));
                }
                map.put(e.getString("oldName"), e.getString("newName"));
            }
            return map;
        } catch (JsonParseException e) {
            throw new ConfigException(format("Unable to parse settings invalid Json: %s", settings), e);
        }
    }

    public List<RegExpSettings> parseRenameRegExpSettings(final String collection) {
        try {
            String settings = getString(FIELD_RENAMER_REGEXP_CONFIG, collection);
            if (settings.isEmpty()) {
                return emptyList();
            }

            Document regexDocument = Document.parse(format("{r: %s}", settings));
            List<RegExpSettings> regExpSettings = new ArrayList<>();
            regexDocument.get("r", Collections.<Document>emptyList()).forEach(d -> regExpSettings.add(new RegExpSettings(d)));
            return regExpSettings;
        } catch (Exception e) {
            throw new ConfigException("Error: parsing rename regexp settings failed", e);
        }
    }

    public IdStrategy getIdStrategy(final String collection) {
        Set<String> availableIdStrategies = getPredefinedIdStrategyClassNames();

        Set<String> customIdStrategies = new HashSet<>(
                splitConfigEntries(getString(DOCUMENT_ID_STRATEGIES_CONFIG))
        );

        availableIdStrategies.addAll(customIdStrategies);

        String strategyClassName = getString(DOCUMENT_ID_STRATEGY_CONFIG, collection);

        if (!availableIdStrategies.contains(strategyClassName)) {
            throw new ConfigException(format("Error: unknown id strategy: %s", strategyClassName));
        }

        try {
            if (strategyClassName.equals(PartialKeyStrategy.class.getName())
                    || strategyClassName.equals(PartialValueStrategy.class.getName())) {
                return (IdStrategy) Class.forName(strategyClassName).getConstructor(FieldProjector.class)
                        .newInstance(this.getKeyProjector(collection));
            }
            return (IdStrategy) Class.forName(strategyClassName).getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(), e);
        } catch (ClassCastException e) {
            throw new ConfigException(format("Error: specified class '%s' violates the contract since it doesn't implement %s",
                    strategyClassName, IdStrategy.class.getSimpleName()));
        }
    }

    public ConnectionString getConnectionString() {
        return new ConnectionString(getString(CONNECTION_URI_CONFIG));
    }

    public String getDatabaseName() {
        String databaseName = getString(DATABASE_NAME_CONFIG);
        if (databaseName.isEmpty()) {
            databaseName = getConnectionString().getDatabase();
        }
        if (databaseName == null || databaseName.isEmpty()) {
            throw new ConfigException(format("Error: No database name specified in either: '%s' or '%s'",
                    DATABASE_NAME_CONFIG, CONNECTION_URI_CONFIG));
        }
        return databaseName;
    }

    Map<String, PostProcessor> buildPostProcessorChains() {
        Map<String, PostProcessor> postProcessorChains = new HashMap<>();
        postProcessorChains.put(TOPIC_AGNOSTIC_KEY_NAME, buildPostProcessorChain(""));
        splitConfigEntries(getString(COLLECTIONS_CONFIG)).forEach(c -> postProcessorChains.put(c, buildPostProcessorChain(c)));
        return postProcessorChains;
    }

    boolean isUsingCdcHandler(final String collection) {
        return !getString(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, collection).isEmpty();
    }

    boolean isDeleteOnNullValues(final String collection) {
        return getBoolean(DELETE_ON_NULL_VALUES_CONFIG, collection);
    }

    WriteModelStrategy getWriteModelStrategy(final String collection) {
        String strategyClassName = getString(WRITEMODEL_STRATEGY_CONFIG, collection);
        try {
            return (WriteModelStrategy) Class.forName(strategyClassName).getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(), e);
        } catch (ClassCastException e) {
            throw new ConfigException(format("Error: Specified class '%s' violates the contract since it doesn't implement: '%s'",
                    strategyClassName, WriteModelStrategy.class.getSimpleName()));
        }
    }

    Map<String, WriteModelStrategy> getWriteModelStrategies() {
        Map<String, WriteModelStrategy> writeModelStrategies = new HashMap<>();
        writeModelStrategies.put(TOPIC_AGNOSTIC_KEY_NAME, getWriteModelStrategy(""));
        splitConfigEntries(getString(COLLECTIONS_CONFIG)).forEach(c -> writeModelStrategies.put(c, getWriteModelStrategy(c)));
        return writeModelStrategies;
    }


    Map<String, RateLimitSettings> getRateLimitSettings() {
        Map<String, RateLimitSettings> rateLimitSettings = new HashMap<>();
        rateLimitSettings.put(TOPIC_AGNOSTIC_KEY_NAME, getRateLimitSettings(""));
        splitConfigEntries(getString(COLLECTIONS_CONFIG)).forEach(c -> rateLimitSettings.put(c, getRateLimitSettings(c)));
        return rateLimitSettings;
    }

    Map<String, WriteModelStrategy> getDeleteOneModelDefaultStrategies() {
        Map<String, WriteModelStrategy> deleteModelStrategies = new HashMap<>();
        if (isDeleteOnNullValues("")) {
            deleteModelStrategies.put(TOPIC_AGNOSTIC_KEY_NAME, getDeleteOneModelDefaultStrategy(""));
        }

        splitConfigEntries(getString(COLLECTIONS_CONFIG)).forEach(collection -> {
            if (isDeleteOnNullValues(collection)) {
                deleteModelStrategies.put(collection, getDeleteOneModelDefaultStrategy(collection));
            }
        });
        return deleteModelStrategies;
    }

    Map<String, CdcHandler> getCdcHandlers() {
        Map<String, CdcHandler> cdcHandlers = new HashMap<>();
        splitConfigEntries(getString(COLLECTIONS_CONFIG)).forEach(collection -> {
            CdcHandler candidate = cdcHandlers.put(collection, getCdcHandler(collection));
            if (candidate != null) {
                cdcHandlers.put(collection, candidate);
            }
        });
        return cdcHandlers;
    }

    Set<String> buildProjectionList(final String projectionType, final String fieldList) {
        if (projectionType.equalsIgnoreCase(FieldProjectionTypes.NONE.name())) {
            return new HashSet<>();
        }

        if (projectionType.equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name())) {
            return new HashSet<>(splitConfigEntries(fieldList));
        }

        if (projectionType.equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name())) {
            //NOTE: for sub document notation all left prefix bound paths are created
            //which allows for easy recursion mechanism to whitelist nested doc fields

            HashSet<String> whitelistExpanded = new HashSet<>();
            List<String> fields = splitConfigEntries(fieldList);

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
        }
        throw new ConfigException(format("Error: invalid settings for: %s ", projectionType));
    }

    List<String> splitConfigEntries(final String configEntries) {
        return Arrays.stream(configEntries.trim().split(FIELD_LIST_SPLIT_EXPR)).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    RateLimitSettings getRateLimitSettings(final String collection) {
        return new RateLimitSettings(getInt(RATE_LIMITING_TIMEOUT_CONFIG, collection), getInt(RATE_LIMITING_EVERY_N_CONFIG, collection));
    }

    WriteModelStrategy getDeleteOneModelDefaultStrategy(final String collection) {

        //NOTE: DeleteOneModel requires the key document
        //which means that the only reasonable ID generation strategies
        //are those which refer to/operate on the key document.
        //Thus currently this means the IdStrategy must be either:

        //FullKeyStrategy
        //PartialKeyStrategy
        //ProvidedInKeyStrategy

        IdStrategy idStrategy = this.getIdStrategy(collection);

        if (!(idStrategy instanceof FullKeyStrategy) && !(idStrategy instanceof PartialKeyStrategy)
                && !(idStrategy instanceof ProvidedInKeyStrategy)) {
            throw new ConfigException(format("Error: %s can only be applied when the configured IdStrategy is an instance of: "
                            + "%s or %s or %s", DeleteOneDefaultStrategy.class.getSimpleName(), FullKeyStrategy.class.getSimpleName(),
                    PartialKeyStrategy.class.getSimpleName(), ProvidedInKeyStrategy.class.getSimpleName()));
        }
        return new DeleteOneDefaultStrategy(idStrategy);
    }

    PostProcessor buildPostProcessorChain(final String collection) {
        Set<String> classes = new LinkedHashSet<>(splitConfigEntries(getString(POST_PROCESSOR_CHAIN_CONFIG, collection)));

        //if no post processors are specified
        //DocumentIdAdder is always used since it's mandatory
        if (classes.size() == 0) {
            return new DocumentIdAdder(this, collection);
        }

        PostProcessor first = null;
        if (!classes.contains(DocumentIdAdder.class.getName())) {
            first = new DocumentIdAdder(this, collection);
        }

        PostProcessor next = null;
        for (String clazz : classes) {
            try {
                if (first == null) {
                    first = (PostProcessor) Class.forName(clazz)
                            .getConstructor(MongoSinkConnectorConfig.class, String.class)
                            .newInstance(this, collection);
                } else {
                    PostProcessor current = (PostProcessor) Class.forName(clazz)
                            .getConstructor(MongoSinkConnectorConfig.class, String.class)
                            .newInstance(this, collection);
                    if (next == null) {
                        first.chain(current);
                        next = current;
                    } else {
                        next = next.chain(current);
                    }
                }
            } catch (ReflectiveOperationException e) {
                throw new ConfigException(e.getMessage(), e);
            } catch (ClassCastException e) {
                throw new ConfigException(format("Error: specified class '%s' violates the contract since it doesn't extend '%s'.",
                        clazz, PostProcessor.class.getSimpleName()));
            }
        }

        return first;
    }

    CdcHandler getCdcHandler(final String collection) {
        Set<String> predefinedCdcHandler = getPredefinedCdcHandlerClassNames();
        String cdcHandler = getString(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, collection);

        if (cdcHandler.isEmpty()) {
            return null;
        } else if (!predefinedCdcHandler.contains(cdcHandler)) {
            throw new ConfigException(format("Error: unknown cdc handler : %s", cdcHandler));
        }

        try {
            return (CdcHandler) Class.forName(cdcHandler).getConstructor(MongoSinkConnectorConfig.class).newInstance(this);
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(), e);
        } catch (ClassCastException e) {
            throw new ConfigException(format("Error: specified class '%s' violates the contract since it doesn't implement %s",
                    cdcHandler, CdcHandler.class.getSimpleName()));
        }

    }

    FieldProjector getKeyProjector(final String collection) {
        if (getString(KEY_PROJECTION_TYPE_CONFIG, collection).equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name())) {
            if (getString(DOCUMENT_ID_STRATEGY_CONFIG, collection).equals(PartialValueStrategy.class.getName())) {
                return new BlacklistValueProjector(this, getKeyProjectionList(collection),
                        cfg -> cfg.isUsingBlacklistKeyProjection(collection), collection);
            } else if (getString(DOCUMENT_ID_STRATEGY_CONFIG, collection).equals(PartialKeyStrategy.class.getName())) {
                return new BlacklistKeyProjector(this, getKeyProjectionList(collection),
                        cfg -> cfg.isUsingBlacklistKeyProjection(collection), collection);
            }
        }

        if (getString(KEY_PROJECTION_TYPE_CONFIG, collection).equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name())) {

            if (getString(DOCUMENT_ID_STRATEGY_CONFIG, collection).equals(PartialValueStrategy.class.getName())) {
                return new WhitelistValueProjector(this, getKeyProjectionList(collection),
                        cfg -> cfg.isUsingWhitelistKeyProjection(collection), collection);
            } else if (getString(DOCUMENT_ID_STRATEGY_CONFIG, collection).equals(PartialKeyStrategy.class.getName())) {
                return new WhitelistKeyProjector(this, getKeyProjectionList(collection),
                        cfg -> cfg.isUsingWhitelistKeyProjection(collection), collection);
            }
        }

        throw new ConfigException(format("Error: settings invalid for %s", KEY_PROJECTION_TYPE_CONFIG));
    }

    static final ConfigDef CONFIG = baseConfigDef();

    private static ConfigDef baseConfigDef() {
        String connection = "Connection";
        String writes = "write config";
        String transformation = "transformation";
        String generation = "generation";

        return new ConfigDef() {

            private <T> Validator<T> ensureValid(final String name, final Consumer<T> consumer) {
                return new Validator<>(name, consumer);
            }

            class Validator<T> {
                private final String name;
                private final Consumer<T> consumer;

                Validator(final String name, final Consumer<T> consumer) {
                    this.name = name;
                    this.consumer = consumer;
                }

                private Validator<T> unless(final boolean condition) {
                    return condition ? new Validator<T>(name, (T t) -> {
                    }) : this;
                }

                private void accept(final T obj) {
                    this.consumer.accept(obj);
                }
            }

            @Override
            public Map<String, ConfigValue> validateAll(final Map<String, String> props) {
                Map<String, ConfigValue> result = super.validateAll(props);
                MongoSinkConnectorConfig config = new MongoSinkConnectorConfig(props);
                Stream.of(
                        ensureValid(CONNECTION_URI_CONFIG, MongoSinkConnectorConfig::getConnectionString),
                        ensureValid(DATABASE_NAME_CONFIG, MongoSinkConnectorConfig::getDatabaseName),
                        ensureValid(KEY_PROJECTION_TYPE_CONFIG, (MongoSinkConnectorConfig cfg) -> cfg.getKeyProjectionList("")),
                        ensureValid(VALUE_PROJECTION_TYPE_CONFIG, (MongoSinkConnectorConfig cfg) -> cfg.getValueProjectionList("")),
                        ensureValid(FIELD_RENAMER_MAPPING_CONFIG, (MongoSinkConnectorConfig cfg) -> cfg.parseRenameFieldnameMappings("")),
                        ensureValid(FIELD_RENAMER_REGEXP_CONFIG, (MongoSinkConnectorConfig cfg) -> cfg.parseRenameRegExpSettings("")),
                        ensureValid(POST_PROCESSOR_CHAIN_CONFIG, (MongoSinkConnectorConfig cfg) -> cfg.buildPostProcessorChain("")),
                        ensureValid(CHANGE_DATA_CAPTURE_HANDLER_CONFIG, (MongoSinkConnectorConfig cfg) -> cfg.getCdcHandler(""))
                                .unless(config.getString(CHANGE_DATA_CAPTURE_HANDLER_CONFIG).isEmpty()),
                        ensureValid(DOCUMENT_ID_STRATEGIES_CONFIG, (MongoSinkConnectorConfig cfg) -> cfg.getIdStrategy(""))
                ).forEach(validator -> {
                    try {
                        validator.accept(config);
                    } catch (Exception ex) {
                        result.get(validator.name).addErrorMessage(ex.getMessage());
                    }
                });
                return result;
            }
        }
        .define(CONNECTION_URI_CONFIG,
            Type.STRING,
            CONNECTION_URI_DEFAULT,
            Importance.HIGH,
            CONNECTION_URI_DOC,
            connection,
            1,
            Width.MEDIUM,
            CONNECTION_URI_DISPLAY)
        .define(DATABASE_NAME_CONFIG,
            Type.STRING,
            DATABASE_DEFAULT,
            Importance.HIGH,
            DATABASE_NAME_DOC,
            connection,
            2,
            Width.MEDIUM,
            DATABASE_NAME_DISPLAY)
        .define(COLLECTION_CONFIG,
            Type.STRING,
            COLLECTION_DEFAULT,
            Importance.HIGH,
            COLLECTION_DOC,
            connection,
            3,
            Width.MEDIUM,
            COLLECTION_DISPLAY)
        .define(COLLECTIONS_CONFIG,
            Type.STRING,
            COLLECTIONS_DEFAULT,
            Importance.MEDIUM,
            COLLECTIONS_DOC,
            connection,
            4,
            Width.MEDIUM,
            COLLECTIONS_DISPLAY)

         // Retry
        .define(MAX_NUM_RETRIES_CONFIG,
            Type.INT,
            MAX_NUM_RETRIES_DEFAULT,
            ConfigDef.Range.atLeast(0),
            Importance.MEDIUM,
            MAX_NUM_RETRIES_DOC,
            writes,
            1,
            Width.MEDIUM,
            "") // Todo
        .define(RETRIES_DEFER_TIMEOUT_CONFIG,
            Type.INT,
            RETRIES_DEFER_TIMEOUT_DEFAULT,
            ConfigDef.Range.atLeast(0),
            Importance.MEDIUM,
            RETRIES_DEFER_TIMEOUT_DOC,
            writes,
            2,
            Width.MEDIUM,
            "") // Todo
        .define(DELETE_ON_NULL_VALUES_CONFIG,
            Type.BOOLEAN,
            DELETE_ON_NULL_VALUES_DEFAULT,
            Importance.MEDIUM,
            DELETE_ON_NULL_VALUES_DOC,
            writes,
            3,
            Width.MEDIUM,
            "") // Todo
        .define(WRITEMODEL_STRATEGY_CONFIG,
            Type.STRING,
            WRITEMODEL_STRATEGY_DEFAULT,
            Importance.LOW,
            WRITEMODEL_STRATEGY_DOC,
            writes,
            3,
            Width.MEDIUM,
            "") // Todo
        .define(MAX_BATCH_SIZE_CONFIG,
            Type.INT,
            MAX_BATCH_SIZE_DEFAULT,
            ConfigDef.Range.atLeast(0),
            Importance.MEDIUM,
            MAX_BATCH_SIZE_DOC,
            writes,
            4,
            Width.MEDIUM,
            "") // Todo
        .define(RATE_LIMITING_TIMEOUT_CONFIG,
            Type.INT,
            RATE_LIMITING_TIMEOUT_DEFAULT,
            ConfigDef.Range.atLeast(0),
            Importance.LOW,
            RATE_LIMITING_TIMEOUT_DOC,
                writes,
                6,
                Width.MEDIUM,
                "") // Todo
        .define(RATE_LIMITING_EVERY_N_CONFIG,
            Type.INT,
            RATE_LIMITING_EVERY_N_DEFAULT,
            ConfigDef.Range.atLeast(0),
            Importance.LOW,
            RATE_LIMITING_EVERY_N_DOC,
                writes,
                7,
                Width.MEDIUM,
                "") // Todo


            // Transformations
        .define(KEY_PROJECTION_TYPE_CONFIG,
                Type.STRING,
                KEY_PROJECTION_TYPE_DEFAULT,
                EnumValidator.in(FieldProjectionTypes.values()),
                Importance.LOW,
                KEY_PROJECTION_TYPE_DOC,
                transformation,
                0,
                Width.MEDIUM,
                "") // Todo
        .define(KEY_PROJECTION_LIST_CONFIG,
                Type.STRING,
                KEY_PROJECTION_LIST_DEFAULT,
                Importance.LOW,
                KEY_PROJECTION_LIST_DOC,
                transformation,
                1,
                Width.MEDIUM,
                "") // Todo
        .define(VALUE_PROJECTION_TYPE_CONFIG,
                Type.STRING,
                VALUE_PROJECTION_TYPE_DEFAULT,
                EnumValidator.in(FieldProjectionTypes.values()),
                Importance.LOW,
                VALUE_PROJECTION_TYPE_DOC,
                transformation,
                2,
                Width.MEDIUM,
                "") // Todo
        .define(VALUE_PROJECTION_LIST_CONFIG,
                Type.STRING,
                VALUE_PROJECTION_LIST_DEFAULT,
                Importance.LOW,
                VALUE_PROJECTION_LIST_DOC,
                transformation,
                3,
                Width.MEDIUM,
                "") // Todo
        .define(FIELD_RENAMER_MAPPING_CONFIG,
                Type.STRING,
                FIELD_RENAMER_MAPPING_DEFAULT,
                Importance.LOW,
                FIELD_RENAMER_MAPPING_DOC,
                transformation,
                4,
                Width.MEDIUM,
                "") // Todo
        .define(FIELD_RENAMER_REGEXP_CONFIG,
                Type.STRING,
                FIELD_RENAMER_REGEXP_DEFAULT,
                Importance.LOW,
                FIELD_RENAMER_REGEXP_DOC,
                transformation,
                5,
                Width.MEDIUM,
                "") // Todo
        .define(POST_PROCESSOR_CHAIN_CONFIG,
                Type.STRING,
                POST_PROCESSOR_CHAIN_DEFAULT,
                emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME_LIST)),
                Importance.LOW,
                POST_PROCESSOR_CHAIN_DOC,
                transformation,
                6,
                Width.MEDIUM,
                "") // Todo

        // Id strategies
        .define(DOCUMENT_ID_STRATEGY_CONFIG,
                Type.STRING,
                DOCUMENT_ID_STRATEGY_DEFAULT,
                emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME)),
                Importance.HIGH,
                DOCUMENT_ID_STRATEGY_CONF_DOC,
                generation,
                1,
                Width.MEDIUM,
                "") // Todo
        .define(DOCUMENT_ID_STRATEGIES_CONFIG,
                Type.STRING,
                DOCUMENT_ID_STRATEGIES_DEFAULT,
                emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME_LIST)),
                Importance.LOW,
                DOCUMENT_ID_STRATEGIES_CONF_DOC,
                generation,
                2,
                Width.MEDIUM,
                "") // Todo

        .define(CHANGE_DATA_CAPTURE_HANDLER_CONFIG,
                Type.STRING,
                CHANGE_DATA_CAPTURE_HANDLER_DEFAULT,
                emptyString().or(matching(FULLY_QUALIFIED_CLASS_NAME)),
                Importance.LOW,
                CHANGE_DATA_CAPTURE_HANDLER_DOC,
                "cdc",
                0,
                Width.MEDIUM,
                ""); // Todo
    }

    static Set<String> getPredefinedCdcHandlerClassNames() {
        Set<String> cdcHandlers = new HashSet<>();
        cdcHandlers.add(MongoDbHandler.class.getName());
        cdcHandlers.add(RdbmsHandler.class.getName());
        cdcHandlers.add(MysqlHandler.class.getName());
        cdcHandlers.add(PostgresHandler.class.getName());
        return cdcHandlers;
    }

    static Set<String> getPredefinedIdStrategyClassNames() {
        Set<String> strategies = new HashSet<String>();
        strategies.add(BsonOidStrategy.class.getName());
        strategies.add(FullKeyStrategy.class.getName());
        strategies.add(KafkaMetaDataStrategy.class.getName());
        strategies.add(PartialKeyStrategy.class.getName());
        strategies.add(PartialValueStrategy.class.getName());
        strategies.add(ProvidedInKeyStrategy.class.getName());
        strategies.add(ProvidedInValueStrategy.class.getName());
        strategies.add(UuidStrategy.class.getName());
        return strategies;
    }

    //EnumValidator
    //https://github.com/confluentinc/kafka-connect-jdbc/blob/master/src/main/java/io/confluent/connect/jdbc/sink/JdbcSinkConfig.java
    private static final class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(final List<String> canonicalValues, final Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(final E[] enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length * 2);
            for (E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toUpperCase());
                validValues.add(e.toString().toLowerCase());
            }
            return new EnumValidator(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(final String key, final Object value) {
            if (!validValues.contains(value)) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }

    interface ValidatorWithOperators extends ConfigDef.Validator {
        default ValidatorWithOperators or(final ConfigDef.Validator other) {
            return (name, value) -> {
                try {
                    this.ensureValid(name, value);
                } catch (ConfigException e) {
                    other.ensureValid(name, value);
                }
            };
        }

        default ValidatorWithOperators and(final ConfigDef.Validator other) {
            return (name, value) -> {
                this.ensureValid(name, value);
                other.ensureValid(name, value);
            };
        }
    }

    static ValidatorWithOperators emptyString() {
        return (name, value) -> {
            // value type already validated when parsed as String, hence ignoring ClassCastException
            if (!((String) value).isEmpty()) {
                throw new ConfigException(name, value, "Not empty");
            }
        };
    }

    static ValidatorWithOperators matching(final Pattern pattern) {
        return (name, value) -> {
            // type already validated when parsing config, hence ignoring ClassCastException
            if (!pattern.matcher((String) value).matches()) {
                throw new ConfigException(name, value, "Does not match: " + pattern);
            }
        };
    }

}
