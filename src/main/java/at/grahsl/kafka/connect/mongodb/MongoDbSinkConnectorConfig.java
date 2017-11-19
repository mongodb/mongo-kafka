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

package at.grahsl.kafka.connect.mongodb;

import at.grahsl.kafka.connect.mongodb.cdc.CdcHandler;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.mongodb.MongoDbHandler;
import at.grahsl.kafka.connect.mongodb.cdc.debezium.mysql.MysqlHandler;
import at.grahsl.kafka.connect.mongodb.processor.*;
import at.grahsl.kafka.connect.mongodb.processor.field.projection.FieldProjector;
import at.grahsl.kafka.connect.mongodb.processor.field.renaming.FieldnameMapping;
import at.grahsl.kafka.connect.mongodb.processor.field.renaming.RegExpSettings;
import at.grahsl.kafka.connect.mongodb.processor.field.renaming.RenameByRegExp;
import at.grahsl.kafka.connect.mongodb.processor.id.strategy.*;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientURI;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class MongoDbSinkConnectorConfig extends AbstractConfig {

    public enum FieldProjectionTypes {
        NONE,
        BLACKLIST,
        WHITELIST
    }

    public static final String FIELD_LIST_SPLIT_CHAR = ",";

    public static final String MONGODB_CONNECTION_URI_DEFAULT = "mongodb://localhost:27017/kafkaconnect?w=1&journal=true";
    public static final String MONGODB_COLLECTION_DEFAULT = "kafkatopic";
    public static final int MONGODB_MAX_NUM_RETRIES_DEFAULT = 3;
    public static final int MONGODB_RETRIES_DEFER_TIMEOUT_DEFAULT = 5000;
    public static final String MONGODB_VALUE_PROJECTION_TYPE_DEFAULT = "none";
    public static final String MONGODB_VALUE_PROJECTION_LIST_DEFAULT = "";
    public static final String MONGODB_DOCUMENT_ID_STRATEGY_DEFAULT = "at.grahsl.kafka.connect.mongodb.processor.id.strategy.BsonOidStrategy";
    public static final String MONGODB_DOCUMENT_ID_STRATEGIES_DEFAULT = "";
    public static final String MONGODB_KEY_PROJECTION_TYPE_DEFAULT = "none";
    public static final String MONGODB_KEY_PROJECTION_LIST_DEFAULT = "";
    public static final String MONGODB_FIELD_RENAMER_MAPPING_DEFAULT = "[]";
    public static final String MONGODB_FIELD_RENAMER_REGEXP_DEFAULT = "[]";
    public static final String MONGODB_POST_PROCESSOR_CHAIN_DEFAULT = "at.grahsl.kafka.connect.mongodb.processor.DocumentIdAdder";
    public static final String MONGODB_CHANGE_DATA_CAPTURE_HANDLER_DEFAULT = "";

    public static final String MONGODB_CONNECTION_URI_CONF = "mongodb.connection.uri";
    private static final String MONGODB_CONNECTION_URI_DOC = "the monogdb connection URI as supported by the offical drivers";

    public static final String MONGODB_COLLECTION_CONF = "mongodb.collection";
    private static final String MONGODB_COLLECTION_DOC = "single sink collection name to write to";

    public static final String MONGODB_MAX_NUM_RETRIES_CONF = "mongodb.max.num.retries";
    private static final String MONGODB_MAX_NUM_RETRIES_DOC = "how often a retry should be done on write errors";

    public static final String MONGODB_RETRIES_DEFER_TIMEOUT_CONF = "mongodb.retries.defer.timeout";
    private static final String MONGODB_RETRIES_DEFER_TIME_OUT_DOC = "how long in ms a retry should get deferred";

    public static final String MONGODB_VALUE_PROJECTION_TYPE_CONF = "mongodb.value.projection.type";
    private static final String MONGODB_VALUE_PROJECTION_TYPE_DOC = "whether or not and which value projection to use";

    public static final String MONGODB_VALUE_PROJECTION_LIST_CONF = "mongodb.value.projection.list";
    private static final String MONGODB_VALUE_PROJECTION_LIST_DOC = "comma separated list of field names for value projection";

    public static final String MONGODB_DOCUMENT_ID_STRATEGY_CONF = "mongodb.document.id.strategy";
    private static final String MONGODB_DOCUMENT_ID_STRATEGY_CONF_DOC = "class name of strategy to use for generating a unique document id (_id)";

    public static final String MONGODB_DOCUMENT_ID_STRATEGIES_CONF = "mongodb.document.id.strategies";
    private static final String MONGODB_DOCUMENT_ID_STRATEGIES_CONF_DOC = "comma separated list of custom strategy classes to register for usage";

    public static final String MONGODB_KEY_PROJECTION_TYPE_CONF = "mongodb.key.projection.type";
    private static final String MONGODB_KEY_PROJECTION_TYPE_DOC = "whether or not and which key projection to use";

    public static final String MONGODB_KEY_PROJECTION_LIST_CONF = "mongodb.key.projection.list";
    private static final String MONGODB_KEY_PROJECTION_LIST_DOC = "comma separated list of field names for key projection";

    public static final String MONGODB_FIELD_RENAMER_MAPPING = "mongodb.field.renamer.mapping";
    private static final String MONGODB_FIELD_RENAMER_MAPPING_DOC = "inline JSON array with objects describing field name mappings (see docs)";

    public static final String MONGODB_FIELD_RENAMER_REGEXP = "mongodb.field.renamer.regexp";
    private static final String MONGODB_FIELD_RENAMER_REGEXP_DOC = "inline JSON array with objects describing regexp settings (see docs)";

    public static final String MONGODB_POST_PROCESSOR_CHAIN = "mongodb.post.processor.chain";
    private static final String MONGODB_POST_PROCESSOR_CHAIN_DOC = "comma separated list of post processor classes to build the chain with";

    public static final String MONGODB_CHANGE_DATA_CAPTURE_HANDLER = "mongodb.change.data.capture.handler";
    private static final String MONGODB_CHANGE_DATA_CAPTURE_HANDLER_DOC = "class name of CDC handler to use for processing";

    private static ObjectMapper objectMapper = new ObjectMapper();

    public MongoDbSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public MongoDbSinkConnectorConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        return new ConfigDef()
                .define(MONGODB_CONNECTION_URI_CONF, Type.STRING, MONGODB_CONNECTION_URI_DEFAULT, Importance.HIGH, MONGODB_CONNECTION_URI_DOC)
                .define(MONGODB_COLLECTION_CONF, Type.STRING, MONGODB_COLLECTION_DEFAULT, Importance.HIGH, MONGODB_COLLECTION_DOC)
                .define(MONGODB_MAX_NUM_RETRIES_CONF, Type.INT, MONGODB_MAX_NUM_RETRIES_DEFAULT, ConfigDef.Range.atLeast(0), Importance.MEDIUM, MONGODB_MAX_NUM_RETRIES_DOC)
                .define(MONGODB_RETRIES_DEFER_TIMEOUT_CONF, Type.INT, MONGODB_RETRIES_DEFER_TIMEOUT_DEFAULT, ConfigDef.Range.atLeast(0), Importance.MEDIUM, MONGODB_RETRIES_DEFER_TIME_OUT_DOC)
                .define(MONGODB_VALUE_PROJECTION_TYPE_CONF, Type.STRING, MONGODB_VALUE_PROJECTION_TYPE_DEFAULT, EnumValidator.in(FieldProjectionTypes.values()), Importance.LOW, MONGODB_VALUE_PROJECTION_TYPE_DOC)
                .define(MONGODB_VALUE_PROJECTION_LIST_CONF, Type.STRING, MONGODB_VALUE_PROJECTION_LIST_DEFAULT, Importance.LOW, MONGODB_VALUE_PROJECTION_LIST_DOC)
                .define(MONGODB_DOCUMENT_ID_STRATEGY_CONF, Type.STRING, MONGODB_DOCUMENT_ID_STRATEGY_DEFAULT, Importance.HIGH, MONGODB_DOCUMENT_ID_STRATEGY_CONF_DOC)
                .define(MONGODB_DOCUMENT_ID_STRATEGIES_CONF, Type.STRING, MONGODB_DOCUMENT_ID_STRATEGIES_DEFAULT, Importance.LOW, MONGODB_DOCUMENT_ID_STRATEGIES_CONF_DOC)
                .define(MONGODB_KEY_PROJECTION_TYPE_CONF, Type.STRING, MONGODB_KEY_PROJECTION_TYPE_DEFAULT, EnumValidator.in(FieldProjectionTypes.values()), Importance.LOW, MONGODB_KEY_PROJECTION_TYPE_DOC)
                .define(MONGODB_KEY_PROJECTION_LIST_CONF, Type.STRING, MONGODB_KEY_PROJECTION_LIST_DEFAULT, Importance.LOW, MONGODB_KEY_PROJECTION_LIST_DOC)
                .define(MONGODB_FIELD_RENAMER_MAPPING, Type.STRING, MONGODB_FIELD_RENAMER_MAPPING_DEFAULT, Importance.LOW, MONGODB_FIELD_RENAMER_MAPPING_DOC)
                .define(MONGODB_FIELD_RENAMER_REGEXP, Type.STRING, MONGODB_FIELD_RENAMER_REGEXP_DEFAULT, Importance.LOW, MONGODB_FIELD_RENAMER_REGEXP_DOC)
                .define(MONGODB_POST_PROCESSOR_CHAIN, Type.STRING, MONGODB_POST_PROCESSOR_CHAIN_DEFAULT, Importance.LOW, MONGODB_POST_PROCESSOR_CHAIN_DOC)
                .define(MONGODB_CHANGE_DATA_CAPTURE_HANDLER, Type.STRING, MONGODB_CHANGE_DATA_CAPTURE_HANDLER_DEFAULT, Importance.LOW, MONGODB_CHANGE_DATA_CAPTURE_HANDLER_DOC)
                ;
    }

    public MongoClientURI buildClientURI() {
        return new MongoClientURI(getString(MONGODB_CONNECTION_URI_CONF));
    }

    public boolean isUsingBlacklistValueProjection() {
        return getString(MONGODB_VALUE_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name());
    }

    public boolean isUsingWhitelistValueProjection() {
        return getString(MONGODB_VALUE_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name());
    }

    public boolean isUsingBlacklistKeyProjection() {
        return getString(MONGODB_KEY_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name());
    }

    public boolean isUsingWhitelistKeyProjection() {
        return getString(MONGODB_KEY_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name());
    }

    public Set<String> getKeyProjectionList() {
        return buildProjectionList(getString(MONGODB_KEY_PROJECTION_TYPE_CONF),
                    getString(MONGODB_KEY_PROJECTION_LIST_CONF)
        );
    }

    public Set<String> getValueProjectionList() {
        return buildProjectionList(getString(MONGODB_VALUE_PROJECTION_TYPE_CONF),
                getString(MONGODB_VALUE_PROJECTION_LIST_CONF)
        );
    }

    public Map<String, String> parseRenameFieldnameMappings() {
        try {
            String settings = getString(MONGODB_FIELD_RENAMER_MAPPING);
            if(settings.isEmpty()) {
                return new HashMap<>();
            }

            List<FieldnameMapping> fm = objectMapper.readValue(
                    settings, new TypeReference<List<FieldnameMapping>>() {});

            Map<String, String> map = new HashMap<>();
            for (FieldnameMapping e : fm) {
                map.put(e.oldName, e.newName);
            }
            return map;
        } catch (IOException e) {
            throw new ConfigException("error: parsing rename fieldname mappings failed", e);
        }
    }

    public Map<String, RenameByRegExp.PatternReplace> parseRenameRegExpSettings() {
        try {
            String settings = getString(MONGODB_FIELD_RENAMER_REGEXP);
            if(settings.isEmpty()) {
                return new HashMap<>();
            }

            List<RegExpSettings> fm = objectMapper.readValue(
                    settings, new TypeReference<List<RegExpSettings>>() {});

            Map<String, RenameByRegExp.PatternReplace> map = new HashMap<>();
            for (RegExpSettings e : fm) {
                map.put(e.regexp, new RenameByRegExp.PatternReplace(e.pattern,e.replace));
            }
            return map;
        } catch (IOException e) {
            throw new ConfigException("error: parsing rename regexp settings failed", e);
        }
    }

    public PostProcessor buildPostProcessorChain() {

        Set<String> classes = new LinkedHashSet<>(
                Arrays.asList(getString(MONGODB_POST_PROCESSOR_CHAIN).split(FIELD_LIST_SPLIT_CHAR))
                    .stream().filter(s -> !s.isEmpty()).collect(Collectors.toList())
        );

        //if no post processors are specified
        //DocumentIdAdder is always used since it's mandatory
        if(classes.size() == 0) {
            return new DocumentIdAdder(this);
        }

        PostProcessor first = null;

        if(!classes.contains(DocumentIdAdder.class.getName())) {
            first = new DocumentIdAdder(this);
        }

        PostProcessor next = null;
        for(String clazz : classes) {
            try {
                if(first == null) {
                    first = (PostProcessor) Class.forName(clazz)
                            .getConstructor(MongoDbSinkConnectorConfig.class)
                            .newInstance(this);
                } else {
                    PostProcessor current = (PostProcessor) Class.forName(clazz)
                            .getConstructor(MongoDbSinkConnectorConfig.class)
                            .newInstance(this);
                    if(next == null) {
                        first.chain(current);
                        next = current;
                    } else {
                        next = next.chain(current);
                    }
                }
            } catch (ReflectiveOperationException e) {
                throw new ConfigException(e.getMessage(),e);
            } catch (ClassCastException e) {
                throw new ConfigException("error: specified class "+ clazz
                        + " violates the contract since it doesn't extend " +
                        PostProcessor.class.getName());
            }
        }

        return first;

    }

    private Set<String> buildProjectionList(String projectionType, String fieldList) {

        if(projectionType.equalsIgnoreCase(FieldProjectionTypes.NONE.name()))
            return new HashSet<>();

        if(projectionType.equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name()))
            return new HashSet<>(Arrays.asList(fieldList.split(FIELD_LIST_SPLIT_CHAR)));

        if(projectionType.equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name())) {

            //NOTE: for sub document notation all left prefix bound paths are created
            //which allows for easy recursion mechanism to whitelist nested doc fields

            HashSet<String> whitelistExpanded = new HashSet<>();
            List<String> fields = Arrays.asList(fieldList.split(FIELD_LIST_SPLIT_CHAR));

            for(String f : fields) {
                if(!f.contains("."))
                    whitelistExpanded.add(f);
                else{
                    String[] parts = f.split("\\.");
                    String entry = parts[0];
                    whitelistExpanded.add(entry);
                    for(int s=1;s<parts.length;s++){
                        entry+="."+parts[s];
                        whitelistExpanded.add(entry);
                    }
                }
            }

            return whitelistExpanded;
        }

        throw new ConfigException("error: invalid settings for "+ projectionType);
    }

    public boolean isUsingCdcHandler() {
        return !getString(MONGODB_CHANGE_DATA_CAPTURE_HANDLER).isEmpty();
    }

    public static Set<String> getPredefinedCdcHandlerClassNames() {
        Set<String> cdcHandlers = new HashSet<String>();
        cdcHandlers.add(MongoDbHandler.class.getName());
        cdcHandlers.add(MysqlHandler.class.getName());
        return cdcHandlers;
    }

    public CdcHandler getCdcHandler() {
        Set<String> predefinedCdcHandler = getPredefinedCdcHandlerClassNames();

        String cdcHandler = getString(MONGODB_CHANGE_DATA_CAPTURE_HANDLER);
        if(!predefinedCdcHandler.contains(cdcHandler)) {
            throw new ConfigException("error: unkown cdc handler "+cdcHandler);
        }

        try {
            return (CdcHandler) Class.forName(cdcHandler)
                    .getConstructor(MongoDbSinkConnectorConfig.class)
                        .newInstance(this);
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(),e);
        } catch (ClassCastException e) {
            throw new ConfigException("error: specified class "+ cdcHandler
                    + " violates the contract since it doesn't implement " +
                    CdcHandler.class);
        }

    }

    public static Set<String> getPredefinedStrategyClassNames() {
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

    public IdStrategy getIdStrategy() {

        Set<String> predefinedStrategies = getPredefinedStrategyClassNames();

        Set<String> customStrategies = Arrays.asList(getString(MONGODB_DOCUMENT_ID_STRATEGIES_CONF)
                        .split(FIELD_LIST_SPLIT_CHAR))
                        .stream().filter(s -> !s.isEmpty()).collect(Collectors.toSet());

        predefinedStrategies.addAll(customStrategies);

        String strategy = getString(MONGODB_DOCUMENT_ID_STRATEGY_CONF);

        if(!predefinedStrategies.contains(strategy)) {
            throw new ConfigException("error: unkown id strategy "+strategy);
        }

        try {
            if(strategy.equals(PartialKeyStrategy.class.getName())
                    || strategy.equals(PartialValueStrategy.class.getName())) {
                return (IdStrategy)Class.forName(strategy)
                        .getConstructor(FieldProjector.class)
                        .newInstance(this.getKeyProjector());
            }
            return (IdStrategy)Class.forName(strategy)
                        .getConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ConfigException(e.getMessage(),e);
        } catch (ClassCastException e) {
            throw new ConfigException("error: specified class "+ strategy
                    + " violates the contract since it doesn't implement " +
                        IdStrategy.class);
        }

    }

    public FieldProjector getKeyProjector() {

        if(getString(MONGODB_KEY_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(FieldProjectionTypes.BLACKLIST.name())) {

            if(getString(MONGODB_DOCUMENT_ID_STRATEGY_CONF).
                    equals(PartialValueStrategy.class.getName())) {

                return new BlacklistValueProjector(this,
                        this.getKeyProjectionList(),cfg -> cfg.isUsingBlacklistKeyProjection());
            }

            if(getString(MONGODB_DOCUMENT_ID_STRATEGY_CONF).
                    equals(PartialKeyStrategy.class.getName())) {

                return new BlacklistKeyProjector(this,
                        this.getKeyProjectionList(),cfg -> cfg.isUsingBlacklistKeyProjection());
            }
        }

        if(getString(MONGODB_KEY_PROJECTION_TYPE_CONF)
                .equalsIgnoreCase(FieldProjectionTypes.WHITELIST.name())) {

            if(getString(MONGODB_DOCUMENT_ID_STRATEGY_CONF).
                    equals(PartialValueStrategy.class.getName())) {

                return new WhitelistValueProjector(this,
                        this.getKeyProjectionList(),cfg -> cfg.isUsingWhitelistKeyProjection());
            }

            if(getString(MONGODB_DOCUMENT_ID_STRATEGY_CONF).
                    equals(PartialKeyStrategy.class.getName())) {

                return new WhitelistKeyProjector(this,
                        this.getKeyProjectionList(),cfg -> cfg.isUsingWhitelistKeyProjection());
            }

        }

        throw new ConfigException("error: settings invalid for "+ MONGODB_KEY_PROJECTION_TYPE_CONF);
    }

    //EnumValidator borrowed from
    //https://github.com/confluentinc/kafka-connect-jdbc/blob/master/src/main/java/io/confluent/connect/jdbc/sink/JdbcSinkConfig.java
    private static class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(E[] enumerators) {
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
        public void ensureValid(String key, Object value) {
            if (!validValues.contains(value)) {
                throw new ConfigException(key, value, "Invalid enumerator");
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }

}
