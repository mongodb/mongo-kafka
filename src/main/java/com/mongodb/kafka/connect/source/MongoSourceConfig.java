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
 */

package com.mongodb.kafka.connect.source;

import static com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults.DEFAULT_AVRO_KEY_SCHEMA;
import static com.mongodb.kafka.connect.source.schema.AvroSchemaDefaults.DEFAULT_AVRO_VALUE_SCHEMA;
import static com.mongodb.kafka.connect.util.ClassHelper.createInstance;
import static com.mongodb.kafka.connect.util.ConfigHelper.collationFromJson;
import static com.mongodb.kafka.connect.util.ConfigHelper.fullDocumentFromString;
import static com.mongodb.kafka.connect.util.ConfigHelper.jsonArrayFromString;
import static com.mongodb.kafka.connect.util.Validators.emptyString;
import static com.mongodb.kafka.connect.util.Validators.errorCheckingValueValidator;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.config.ConfigDef.Width;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigValue;

import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import com.mongodb.ConnectionString;
import com.mongodb.client.model.Collation;
import com.mongodb.client.model.changestream.FullDocument;

import com.mongodb.kafka.connect.source.json.formatter.JsonWriterSettingsProvider;
import com.mongodb.kafka.connect.source.schema.AvroSchema;
import com.mongodb.kafka.connect.source.topic.mapping.TopicMapper;
import com.mongodb.kafka.connect.util.ConfigHelper;
import com.mongodb.kafka.connect.util.ConnectConfigException;
import com.mongodb.kafka.connect.util.Validators;

public class MongoSourceConfig extends AbstractConfig {

  private static final Pattern CLASS_NAME =
      Pattern.compile("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
  private static final Pattern FULLY_QUALIFIED_CLASS_NAME =
      Pattern.compile("(" + CLASS_NAME + "\\.)*" + CLASS_NAME);

  public static final String CONNECTION_URI_CONFIG = "connection.uri";
  private static final String CONNECTION_URI_DEFAULT =
      "mongodb://localhost:27017,localhost:27018,localhost:27019";
  private static final String CONNECTION_URI_DISPLAY = "MongoDB Connection URI";
  private static final String CONNECTION_URI_DOC =
      "The connection URI as supported by the official drivers. "
          + "eg: ``mongodb://user@pass@locahost/``.";

  public static final String OUTPUT_FORMAT_KEY_CONFIG = "output.format.key";
  private static final String OUTPUT_FORMAT_KEY_DEFAULT =
      OutputFormat.JSON.name().toLowerCase(Locale.ROOT);
  private static final String OUTPUT_FORMAT_KEY_DISPLAY = "The key output format";
  private static final String OUTPUT_FORMAT_KEY_DOC =
      "The output format of the data produced by the connector for the key. Supported formats are:\n"
          + " * `json` - Raw Json strings\n"
          + " * `bson` - Bson byte array\n"
          + " * `schema` - Schema'd output\n";

  public static final String OUTPUT_FORMAT_VALUE_CONFIG = "output.format.value";
  private static final String OUTPUT_FORMAT_VALUE_DEFAULT =
      OutputFormat.JSON.name().toLowerCase(Locale.ROOT);
  private static final String OUTPUT_FORMAT_VALUE_DISPLAY = "The value output format";
  private static final String OUTPUT_FORMAT_VALUE_DOC =
      "The output format of the data produced by the connector for the value. Supported formats are:\n"
          + " * `json` - Raw Json strings\n"
          + " * `bson` - Bson byte array\n"
          + " * `schema` - Schema'd output\n";

  public static final String OUTPUT_JSON_FORMATTER_CONFIG = "output.json.formatter";
  private static final String OUTPUT_JSON_FORMATTER_DEFAULT =
      "com.mongodb.kafka.connect.source.json.formatter.DefaultJson";
  private static final String OUTPUT_JSON_FORMATTER_DISPLAY = "The json formatter class";
  private static final String OUTPUT_JSON_FORMATTER_DOC =
      "The output format of json strings can be configured to be either:\n"
          + "  * com.mongodb.kafka.connect.source.json.formatter.DefaultJson: The legacy strict json formatter.\n"
          + "  * com.mongodb.kafka.connect.source.json.formatter.ExtendedJson: The fully type safe extended json formatter.\n"
          + "  * com.mongodb.kafka.connect.source.json.formatter.SimplifiedJson: Simplified Json, "
          + "with ObjectId, Decimals, Dates and Binary values represented as strings.\n\n"
          + "Users can provide their own implementation of the com.mongodb.kafka.connect.source.json.formatter.";

  public static final String OUTPUT_SCHEMA_KEY_CONFIG = "output.schema.key";
  private static final String OUTPUT_SCHEMA_KEY_DEFAULT = DEFAULT_AVRO_KEY_SCHEMA;
  private static final String OUTPUT_SCHEMA_KEY_DISPLAY = "The Avro schema definition for the key.";
  private static final String OUTPUT_SCHEMA_KEY_DOC =
      "The Avro schema definition for the key value of the SourceRecord.";

  public static final String OUTPUT_SCHEMA_VALUE_CONFIG = "output.schema.value";
  private static final String OUTPUT_SCHEMA_VALUE_DEFAULT = DEFAULT_AVRO_VALUE_SCHEMA;
  private static final String OUTPUT_SCHEMA_VALUE_DISPLAY =
      "The Avro schema definition for the value.";
  private static final String OUTPUT_SCHEMA_VALUE_DOC =
      "The Avro schema definition for the value of the SourceRecord.";

  public static final String OUTPUT_SCHEMA_INFER_VALUE_CONFIG = "output.schema.infer.value";
  public static final boolean OUTPUT_SCHEMA_INFER_VALUE_DEFAULT = false;
  private static final String OUTPUT_SCHEMA_INFER_VALUE_DOC =
      "Infer the schema for the value. "
          + "Each Document will be processed in isolation, which may lead to multiple schema definitions "
          + "for the data. Only applied when "
          + OUTPUT_FORMAT_VALUE_CONFIG
          + " is set to schema.";
  private static final String OUTPUT_SCHEMA_INFER_VALUE_DISPLAY =
      "Enable Infer Schemas for the value";

  public static final String TOPIC_MAPPER_CONFIG = "topic.mapper";
  private static final String TOPIC_MAPPER_DISPLAY = "The topic mapper class";
  private static final String TOPIC_MAPPER_DOC =
      "The class that determines the topic to write the source data to. "
          + "By default this will be based on the 'ns' field in the change stream document, "
          + "along with any configured prefix and suffix.";
  private static final String TOPIC_MAPPER_DEFAULT =
      "com.mongodb.kafka.connect.source.topic.mapping.DefaultTopicMapper";

  public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
  private static final String TOPIC_PREFIX_DOC =
      "Prefix to prepend to database & collection names to generate the name of the Kafka "
          + "topic to publish data to. Used by the 'DefaultTopicMapper'.";
  private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";
  private static final String TOPIC_PREFIX_DEFAULT = "";

  public static final String TOPIC_SUFFIX_CONFIG = "topic.suffix";
  private static final String TOPIC_SUFFIX_DOC =
      "Suffix to append to database & collection names to generate the name of the Kafka "
          + "topic to publish data to. Used by the 'DefaultTopicMapper'.";
  private static final String TOPIC_SUFFIX_DISPLAY = "Topic Suffix";
  private static final String TOPIC_SUFFIX_DEFAULT = "";

  public static final String TOPIC_NAMESPACE_MAP_CONFIG = "topic.namespace.map";
  private static final String TOPIC_NAMESPACE_MAP_DISPLAY = "The namespace to topic map";
  private static final String TOPIC_NAMESPACE_MAP_DOC =
      "A json map that maps change stream document namespaces to topics.\n"
          + "For example: `{\"db\": \"dbTopic\", \"db.coll\": \"dbCollTopic\"}` will map all "
          + "change stream documents from the `db` database to `dbTopic.<collectionName>` apart from"
          + "any documents from the `db.coll` namespace which map to the `dbCollTopic` topic.\n"
          + "If you want to map all messages to a single topic use `*`: "
          + "For example: `{\"*\": \"everyThingTopic\", \"db.coll\": \"exceptionToTheRuleTopic\"}` "
          + "will map all change stream documents to the `everyThingTopic` apart from the `db.coll` "
          + "messages."
          + "Note: Any prefix and suffix configuration will still apply.";
  private static final String TOPIC_NAMESPACE_MAP_DEFAULT = "";

  public static final String PIPELINE_CONFIG = "pipeline";
  private static final String PIPELINE_DISPLAY = "The pipeline to apply to the change stream";
  private static final String PIPELINE_DOC =
      "An inline JSON array with objects describing the pipeline operations to run.\n"
          + "Example: `[{\"$match\": {\"operationType\": \"insert\"}}, {\"$addFields\": {\"Kafka\": \"Rules!\"}}]`";
  private static final String PIPELINE_DEFAULT = "[]";

  public static final String BATCH_SIZE_CONFIG = "batch.size";
  private static final String BATCH_SIZE_DISPLAY = "The cursor batch size";
  private static final String BATCH_SIZE_DOC = "The cursor batch size.";
  private static final int BATCH_SIZE_DEFAULT = 0;

  public static final String PUBLISH_FULL_DOCUMENT_ONLY_CONFIG = "publish.full.document.only";
  private static final String PUBLISH_FULL_DOCUMENT_ONLY_DISPLAY =
      "Publish only the `fullDocument` field";
  private static final String PUBLISH_FULL_DOCUMENT_ONLY_DOC =
      "Only publish the actual changed document rather than the full change "
          + "stream document. Automatically, sets `change.stream.full.document=updateLookup` so updated documents will be included.";
  private static final boolean PUBLISH_FULL_DOCUMENT_ONLY_DEFAULT = false;

  public static final String FULL_DOCUMENT_CONFIG = "change.stream.full.document";
  private static final String FULL_DOCUMENT_DISPLAY = "Set what to return for update operations";
  private static final String FULL_DOCUMENT_DOC =
      "Determines what to return for update operations when using a Change Stream.\n"
          + "When set to 'updateLookup', the change stream for partial updates will include both a delta "
          + "describing the changes to the document as well as a copy of the entire document that was changed from *some time* after "
          + "the change occurred.";
  private static final String FULL_DOCUMENT_DEFAULT = "";

  public static final String COLLATION_CONFIG = "collation";
  private static final String COLLATION_DISPLAY = "The collation options";
  private static final String COLLATION_DOC =
      "The json representation of the Collation options to use for the change stream.\n"
          + "Use the `Collation.asDocument().toJson()` to create the specific json representation.";
  private static final String COLLATION_DEFAULT = "";

  public static final String POLL_MAX_BATCH_SIZE_CONFIG = "poll.max.batch.size";
  private static final String POLL_MAX_BATCH_SIZE_DISPLAY = "The maximum batch size";
  private static final String POLL_MAX_BATCH_SIZE_DOC =
      "Maximum number of change stream documents to include in a single batch when "
          + "polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.";
  private static final int POLL_MAX_BATCH_SIZE_DEFAULT = 1000;

  public static final String POLL_AWAIT_TIME_MS_CONFIG = "poll.await.time.ms";
  private static final String POLL_AWAIT_TIME_MS_DOC =
      "The amount of time to wait before checking for new results on the change stream";
  private static final int POLL_AWAIT_TIME_MS_DEFAULT = 5000;
  private static final String POLL_AWAIT_TIME_MS_DISPLAY = "Poll await time (ms)";

  public static final String DATABASE_CONFIG = "database";
  private static final String DATABASE_DISPLAY = "The database to watch.";
  private static final String DATABASE_DOC =
      "The database to watch. If not set then all databases will be watched.";
  private static final String DATABASE_DEFAULT = "";

  public static final String COLLECTION_CONFIG = "collection";
  private static final String COLLECTION_DISPLAY = "The collection to watch.";
  private static final String COLLECTION_DOC =
      "The collection in the database to watch. If not set then all collections will be "
          + "watched.";
  private static final String COLLECTION_DEFAULT = "";

  public static final String COPY_EXISTING_CONFIG = "copy.existing";
  private static final String COPY_EXISTING_DISPLAY = "Copy existing data";
  private static final String COPY_EXISTING_DOC =
      "Copy existing data from all the collections being used as the source then add "
          + "any changes after. It should be noted that the reading of all the data during the copy and then the subsequent change "
          + "stream events may produce duplicated events. During the copy, clients can make changes to the data in MongoDB, which may be "
          + "represented both by the copying process and the change stream. However, as the change stream events are idempotent the "
          + "changes can be applied so that the data is eventually consistent. Renaming a collection during the copying process is not "
          + "supported.";
  private static final boolean COPY_EXISTING_DEFAULT = false;

  public static final String COPY_EXISTING_MAX_THREADS_CONFIG = "copy.existing.max.threads";
  private static final String COPY_EXISTING_MAX_THREADS_DISPLAY =
      "Copy existing max number of threads";
  private static final String COPY_EXISTING_MAX_THREADS_DOC =
      "The number of threads to use when performing the data copy. "
          + "Defaults to the number of processors";
  private static final int COPY_EXISTING_MAX_THREADS_DEFAULT =
      Runtime.getRuntime().availableProcessors();

  public static final String COPY_EXISTING_QUEUE_SIZE_CONFIG = "copy.existing.queue.size";
  private static final String COPY_EXISTING_QUEUE_SIZE_DISPLAY = "Copy existing queue size";
  private static final String COPY_EXISTING_QUEUE_SIZE_DOC =
      "The max size of the queue to use when copying data.";
  private static final int COPY_EXISTING_QUEUE_SIZE_DEFAULT = 16000;

  public static final String COPY_EXISTING_PIPELINE_CONFIG = "copy.existing.pipeline";
  private static final String COPY_EXISTING_PIPELINE_DISPLAY = "Copy existing initial pipeline";
  private static final String COPY_EXISTING_PIPELINE_DOC =
      "An inline JSON array with objects describing the pipeline operations to run when copying existing data.\n"
          + "This can improve the use of indexes by the copying manager and make copying more efficient.\n"
          + "Use if there is any filtering of collection data in the `pipeline` configuration to speed up the copying process.\n"
          + "Example: `[{\"$match\": {\"closed\": \"false\"}}]`";
  private static final String COPY_EXISTING_PIPELINE_DEFAULT = "";

  public static final String COPY_EXISTING_NAMESPACE_REGEX_CONFIG = "copy.existing.namespace.regex";
  private static final String COPY_EXISTING_NAMESPACE_REGEX_DISPLAY =
      "Copy existing namespace regex";
  private static final String COPY_EXISTING_NAMESPACE_REGEX_DOC =
      "Use a regular expression to define from which existing namespaces data should be copied from."
          + " A namespace is the database name and collection separated by a period e.g. `database.collection`.\n"
          + " Example: The following regular expression will only include collections starting with `a` "
          + "in the `demo` database: `demo\\.a.*`";
  private static final String COPY_EXISTING_NAMESPACE_REGEX_DEFAULT = "";

  public static final String ERRORS_TOLERANCE_CONFIG = "errors.tolerance";
  public static final String ERRORS_TOLERANCE_DISPLAY = "Error Tolerance";
  public static final ErrorTolerance ERRORS_TOLERANCE_DEFAULT = ErrorTolerance.NONE;
  public static final String ERRORS_TOLERANCE_DOC =
      "Behavior for tolerating errors during connector operation. 'none' is the default value "
          + "and signals that any error will result in an immediate connector task failure; 'all' "
          + "changes the behavior to skip over problematic records.";

  public static final String ERRORS_LOG_ENABLE_CONFIG = "errors.log.enable";
  public static final String ERRORS_LOG_ENABLE_DISPLAY = "Log Errors";
  public static final boolean ERRORS_LOG_ENABLE_DEFAULT = false;
  public static final String ERRORS_LOG_ENABLE_DOC =
      "If true, write each error and the details of the failed operation and problematic record "
          + "to the Connect application log. This is 'false' by default, so that only errors that are not tolerated are reported.";

  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG =
      "errors.deadletterqueue.topic.name";
  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DISPLAY =
      "Output errors to the dead letter queue";
  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT = "";
  public static final String ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DOC =
      "Whether to output conversion errors to the dead letter queue. "
          + "Stops poison messages when using schemas, any message will be outputted as extended json on the specified topic. "
          + "By default messages are not outputted to the dead letter queue. "
          + "Also requires `errors.tolerance=all`.";

  public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
  private static final String HEARTBEAT_INTERVAL_MS_DISPLAY = "Heartbeat interval";
  private static final String HEARTBEAT_INTERVAL_MS_DOC =
      "The length of time between sending heartbeat messages to record the post batch resume token"
          + " when no source records have been published. Improves the resumability of the connector"
          + " for low volume namespaces. Use 0 to disable.";
  private static final int HEARTBEAT_INTERVAL_MS_DEFAULT = 0;

  public static final String HEARTBEAT_TOPIC_NAME_CONFIG = "heartbeat.topic.name";
  private static final String HEARTBEAT_TOPIC_NAME_DISPLAY = "The heartbeat topic name";
  private static final String HEARTBEAT_TOPIC_NAME_DEFAULT = "__mongodb_heartbeats";
  private static final String HEARTBEAT_TOPIC_NAME_DOC =
      "The name of the topic to publish heartbeats to."
          + " Defaults to '"
          + HEARTBEAT_TOPIC_NAME_DEFAULT
          + "'.";

  public static final String OFFSET_PARTITION_NAME_CONFIG = "offset.partition.name";
  public static final String OFFSET_PARTITION_NAME_DISPLAY = "Offset partition name";
  public static final String OFFSET_PARTITION_NAME_DEFAULT = "";
  public static final String OFFSET_PARTITION_NAME_DOC =
      "Use a custom offset partition name. If blank the default partition name based on the "
          + "connection details will be used.";

  public static final ConfigDef CONFIG = createConfigDef();
  private static final List<Consumer<MongoSourceConfig>> INITIALIZERS =
      asList(MongoSourceConfig::validateCollection, MongoSourceConfig::getTopicMapper);

  public enum OutputFormat {
    JSON,
    BSON,
    SCHEMA
  }

  public enum ErrorTolerance {

    /** Tolerate no errors. */
    NONE,

    /** Tolerate all errors. */
    ALL;

    public String value() {
      return name().toLowerCase(Locale.ROOT);
    }
  }

  private final ConnectionString connectionString;
  private TopicMapper topicMapper;

  public MongoSourceConfig(final Map<?, ?> originals) {
    this(originals, true);
  }

  private MongoSourceConfig(final Map<?, ?> originals, final boolean validateAll) {
    super(CONFIG, originals, false);
    connectionString = new ConnectionString(getString(CONNECTION_URI_CONFIG));

    if (validateAll) {
      INITIALIZERS.forEach(i -> i.accept(this));
    }
  }

  public ConnectionString getConnectionString() {
    return connectionString;
  }

  public OutputFormat getKeyOutputFormat() {
    return OutputFormat.valueOf(getString(OUTPUT_FORMAT_KEY_CONFIG).toUpperCase());
  }

  public OutputFormat getValueOutputFormat() {
    return OutputFormat.valueOf(getString(OUTPUT_FORMAT_VALUE_CONFIG).toUpperCase());
  }

  public Optional<List<Document>> getPipeline() {
    return getPipeline(PIPELINE_CONFIG);
  }

  public Optional<List<Document>> getPipeline(final String configName) {
    return jsonArrayFromString(getString(configName));
  }

  public Optional<Collation> getCollation() {
    return collationFromJson(getString(COLLATION_CONFIG));
  }

  public Optional<FullDocument> getFullDocument() {
    if (getBoolean(PUBLISH_FULL_DOCUMENT_ONLY_CONFIG)) {
      return Optional.of(FullDocument.UPDATE_LOOKUP);
    } else {
      return fullDocumentFromString(getString(FULL_DOCUMENT_CONFIG));
    }
  }

  private void validateCollection() {
    String database = getString(DATABASE_CONFIG);
    String collection = getString(COLLECTION_CONFIG);
    if (!collection.isEmpty() && database.isEmpty()) {
      throw new ConnectConfigException(
          COLLECTION_CONFIG,
          collection,
          format("Missing database configuration `%s`", DATABASE_CONFIG));
    }
  }

  public TopicMapper getTopicMapper() {
    if (topicMapper == null) {
      topicMapper =
          configureInstance(
              createInstance(
                  TOPIC_MAPPER_CONFIG, getString(TOPIC_MAPPER_CONFIG), TopicMapper.class));
    }
    return topicMapper;
  }

  public JsonWriterSettings getJsonWriterSettings() {
    return createInstance(
            OUTPUT_JSON_FORMATTER_CONFIG,
            getString(OUTPUT_JSON_FORMATTER_CONFIG),
            JsonWriterSettingsProvider.class)
        .getJsonWriterSettings();
  }

  public boolean logErrors() {
    return !tolerateErrors() || getBoolean(ERRORS_LOG_ENABLE_CONFIG);
  }

  public boolean tolerateErrors() {
    return ErrorTolerance.valueOf(getString(ERRORS_TOLERANCE_CONFIG).toUpperCase())
        .equals(ErrorTolerance.ALL);
  }

  private <T extends Configurable> T configureInstance(final T instance) {
    instance.configure(this);
    return instance;
  }

  private static ConfigDef createConfigDef() {
    ConfigDef configDef =
        new ConfigDef() {

          @Override
          public Map<String, ConfigValue> validateAll(final Map<String, String> props) {
            Map<String, ConfigValue> results = super.validateAll(props);
            // Don't validate child configs if the top level configs are broken
            if (results.values().stream().anyMatch((c) -> !c.errorMessages().isEmpty())) {
              return results;
            }

            // Validate database & collection
            MongoSourceConfig cfg = new MongoSourceConfig(props, false);
            INITIALIZERS.forEach(
                i -> {
                  try {
                    i.accept(cfg);
                  } catch (ConnectConfigException t) {
                    results.put(
                        t.getName(),
                        new ConfigValue(
                            t.getName(), t.getValue(), emptyList(), singletonList(t.getMessage())));
                  }
                });
            return results;
          }
        };
    String group = "ChangeStream";
    int orderInGroup = 0;
    configDef.define(
        CONNECTION_URI_CONFIG,
        Type.STRING,
        CONNECTION_URI_DEFAULT,
        errorCheckingValueValidator("A valid connection string", ConnectionString::new),
        Importance.HIGH,
        CONNECTION_URI_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        CONNECTION_URI_DISPLAY);

    configDef.define(
        DATABASE_CONFIG,
        Type.STRING,
        DATABASE_DEFAULT,
        Importance.MEDIUM,
        DATABASE_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        DATABASE_DISPLAY);

    configDef.define(
        COLLECTION_CONFIG,
        Type.STRING,
        COLLECTION_DEFAULT,
        Importance.MEDIUM,
        COLLECTION_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        COLLECTION_DISPLAY);

    configDef.define(
        PIPELINE_CONFIG,
        Type.STRING,
        PIPELINE_DEFAULT,
        errorCheckingValueValidator("A valid JSON array", ConfigHelper::jsonArrayFromString),
        Importance.MEDIUM,
        PIPELINE_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        PIPELINE_DISPLAY);

    configDef.define(
        BATCH_SIZE_CONFIG,
        Type.INT,
        BATCH_SIZE_DEFAULT,
        ConfigDef.Range.atLeast(0),
        Importance.MEDIUM,
        BATCH_SIZE_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        BATCH_SIZE_DISPLAY);

    configDef.define(
        PUBLISH_FULL_DOCUMENT_ONLY_CONFIG,
        Type.BOOLEAN,
        PUBLISH_FULL_DOCUMENT_ONLY_DEFAULT,
        Importance.HIGH,
        PUBLISH_FULL_DOCUMENT_ONLY_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        PUBLISH_FULL_DOCUMENT_ONLY_DISPLAY);

    configDef.define(
        FULL_DOCUMENT_CONFIG,
        Type.STRING,
        FULL_DOCUMENT_DEFAULT,
        emptyString()
            .or(
                Validators.EnumValidatorAndRecommender.in(
                    FullDocument.values(), FullDocument::getValue)),
        Importance.HIGH,
        FULL_DOCUMENT_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        FULL_DOCUMENT_DISPLAY,
        Validators.EnumValidatorAndRecommender.in(FullDocument.values(), FullDocument::getValue));

    configDef.define(
        COLLATION_CONFIG,
        Type.STRING,
        COLLATION_DEFAULT,
        errorCheckingValueValidator(
            "A valid JSON document representing a collation", ConfigHelper::collationFromJson),
        Importance.HIGH,
        COLLATION_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        COLLATION_DISPLAY);

    configDef.define(
        POLL_MAX_BATCH_SIZE_CONFIG,
        Type.INT,
        POLL_MAX_BATCH_SIZE_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        POLL_MAX_BATCH_SIZE_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        POLL_MAX_BATCH_SIZE_DISPLAY);

    configDef.define(
        POLL_AWAIT_TIME_MS_CONFIG,
        Type.LONG,
        POLL_AWAIT_TIME_MS_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.LOW,
        POLL_AWAIT_TIME_MS_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        POLL_AWAIT_TIME_MS_DISPLAY);

    group = "Topic mapping";
    orderInGroup = 0;
    configDef.define(
        TOPIC_MAPPER_CONFIG,
        ConfigDef.Type.STRING,
        TOPIC_MAPPER_DEFAULT,
        Validators.matching(FULLY_QUALIFIED_CLASS_NAME),
        ConfigDef.Importance.HIGH,
        TOPIC_MAPPER_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.LONG,
        TOPIC_MAPPER_DISPLAY);

    configDef.define(
        TOPIC_PREFIX_CONFIG,
        Type.STRING,
        TOPIC_PREFIX_DEFAULT,
        null,
        Importance.LOW,
        TOPIC_PREFIX_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        TOPIC_PREFIX_DISPLAY);
    configDef.define(
        TOPIC_SUFFIX_CONFIG,
        Type.STRING,
        TOPIC_SUFFIX_DEFAULT,
        null,
        Importance.LOW,
        TOPIC_SUFFIX_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        TOPIC_SUFFIX_DISPLAY);
    configDef.define(
        TOPIC_NAMESPACE_MAP_CONFIG,
        Type.STRING,
        TOPIC_NAMESPACE_MAP_DEFAULT,
        errorCheckingValueValidator("A valid JSON document", ConfigHelper::documentFromString),
        Importance.HIGH,
        TOPIC_NAMESPACE_MAP_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        TOPIC_NAMESPACE_MAP_DISPLAY);

    group = "Schema";
    orderInGroup = 0;

    configDef.define(
        OUTPUT_FORMAT_KEY_CONFIG,
        ConfigDef.Type.STRING,
        OUTPUT_FORMAT_KEY_DEFAULT,
        Validators.EnumValidatorAndRecommender.in(OutputFormat.values()),
        ConfigDef.Importance.HIGH,
        OUTPUT_FORMAT_KEY_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        OUTPUT_FORMAT_KEY_DISPLAY,
        Validators.EnumValidatorAndRecommender.in(OutputFormat.values()));

    configDef.define(
        OUTPUT_FORMAT_VALUE_CONFIG,
        ConfigDef.Type.STRING,
        OUTPUT_FORMAT_VALUE_DEFAULT,
        Validators.EnumValidatorAndRecommender.in(OutputFormat.values()),
        ConfigDef.Importance.HIGH,
        OUTPUT_FORMAT_VALUE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        OUTPUT_FORMAT_VALUE_DISPLAY,
        Validators.EnumValidatorAndRecommender.in(OutputFormat.values()));

    configDef.define(
        OUTPUT_SCHEMA_INFER_VALUE_CONFIG,
        Type.BOOLEAN,
        OUTPUT_SCHEMA_INFER_VALUE_DEFAULT,
        Importance.MEDIUM,
        OUTPUT_SCHEMA_INFER_VALUE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        OUTPUT_SCHEMA_INFER_VALUE_DISPLAY);

    configDef.define(
        OUTPUT_SCHEMA_KEY_CONFIG,
        ConfigDef.Type.STRING,
        OUTPUT_SCHEMA_KEY_DEFAULT,
        errorCheckingValueValidator(
            "A valid Avro schema definition", AvroSchema::validateJsonSchema),
        ConfigDef.Importance.HIGH,
        OUTPUT_SCHEMA_KEY_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        OUTPUT_SCHEMA_KEY_DISPLAY);

    configDef.define(
        OUTPUT_SCHEMA_VALUE_CONFIG,
        ConfigDef.Type.STRING,
        OUTPUT_SCHEMA_VALUE_DEFAULT,
        errorCheckingValueValidator(
            "A valid Avro schema definition", AvroSchema::validateJsonSchema),
        ConfigDef.Importance.HIGH,
        OUTPUT_SCHEMA_VALUE_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        OUTPUT_SCHEMA_VALUE_DISPLAY);

    configDef.define(
        OUTPUT_JSON_FORMATTER_CONFIG,
        ConfigDef.Type.STRING,
        OUTPUT_JSON_FORMATTER_DEFAULT,
        Validators.matching(FULLY_QUALIFIED_CLASS_NAME),
        Importance.MEDIUM,
        OUTPUT_JSON_FORMATTER_DOC,
        group,
        ++orderInGroup,
        ConfigDef.Width.MEDIUM,
        OUTPUT_JSON_FORMATTER_DISPLAY);

    group = "Copy existing";
    orderInGroup = 0;

    configDef.define(
        COPY_EXISTING_CONFIG,
        Type.BOOLEAN,
        COPY_EXISTING_DEFAULT,
        Importance.MEDIUM,
        COPY_EXISTING_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        COPY_EXISTING_DISPLAY);

    configDef.define(
        COPY_EXISTING_MAX_THREADS_CONFIG,
        Type.INT,
        COPY_EXISTING_MAX_THREADS_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.MEDIUM,
        COPY_EXISTING_MAX_THREADS_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        COPY_EXISTING_MAX_THREADS_DISPLAY);

    configDef.define(
        COPY_EXISTING_QUEUE_SIZE_CONFIG,
        Type.INT,
        COPY_EXISTING_QUEUE_SIZE_DEFAULT,
        ConfigDef.Range.atLeast(1),
        Importance.MEDIUM,
        COPY_EXISTING_QUEUE_SIZE_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        COPY_EXISTING_QUEUE_SIZE_DISPLAY);

    configDef.define(
        COPY_EXISTING_PIPELINE_CONFIG,
        Type.STRING,
        COPY_EXISTING_PIPELINE_DEFAULT,
        errorCheckingValueValidator("A valid JSON array", ConfigHelper::jsonArrayFromString),
        Importance.MEDIUM,
        COPY_EXISTING_PIPELINE_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        COPY_EXISTING_PIPELINE_DISPLAY);

    configDef.define(
        COPY_EXISTING_NAMESPACE_REGEX_CONFIG,
        Type.STRING,
        COPY_EXISTING_NAMESPACE_REGEX_DEFAULT,
        Validators.emptyString().or(Validators.isAValidRegex()),
        Importance.MEDIUM,
        COPY_EXISTING_NAMESPACE_REGEX_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        COPY_EXISTING_NAMESPACE_REGEX_DISPLAY);

    group = "Errors";
    orderInGroup = 0;

    configDef.define(
        ERRORS_TOLERANCE_CONFIG,
        Type.STRING,
        ERRORS_TOLERANCE_DEFAULT.value(),
        Validators.EnumValidatorAndRecommender.in(ErrorTolerance.values()),
        Importance.MEDIUM,
        ERRORS_TOLERANCE_DOC,
        group,
        ++orderInGroup,
        Width.SHORT,
        ERRORS_TOLERANCE_DISPLAY);

    configDef.define(
        ERRORS_LOG_ENABLE_CONFIG,
        Type.BOOLEAN,
        ERRORS_LOG_ENABLE_DEFAULT,
        Importance.MEDIUM,
        ERRORS_LOG_ENABLE_DOC,
        group,
        ++orderInGroup,
        Width.SHORT,
        ERRORS_LOG_ENABLE_DISPLAY);

    configDef.define(
        ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG,
        Type.STRING,
        ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DEFAULT,
        Importance.MEDIUM,
        ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DOC,
        group,
        ++orderInGroup,
        Width.SHORT,
        ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_DISPLAY);

    configDef.define(
        HEARTBEAT_INTERVAL_MS_CONFIG,
        Type.LONG,
        HEARTBEAT_INTERVAL_MS_DEFAULT,
        ConfigDef.Range.atLeast(0),
        Importance.MEDIUM,
        HEARTBEAT_INTERVAL_MS_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        HEARTBEAT_INTERVAL_MS_DISPLAY);

    configDef.define(
        HEARTBEAT_TOPIC_NAME_CONFIG,
        Type.STRING,
        HEARTBEAT_TOPIC_NAME_DEFAULT,
        new ConfigDef.NonEmptyString(),
        Importance.MEDIUM,
        HEARTBEAT_TOPIC_NAME_DOC,
        group,
        ++orderInGroup,
        Width.MEDIUM,
        HEARTBEAT_TOPIC_NAME_DISPLAY);

    group = "Partition";
    orderInGroup = 0;

    configDef.define(
        OFFSET_PARTITION_NAME_CONFIG,
        Type.STRING,
        OFFSET_PARTITION_NAME_DEFAULT,
        Importance.MEDIUM,
        OFFSET_PARTITION_NAME_DOC,
        group,
        ++orderInGroup,
        Width.SHORT,
        OFFSET_PARTITION_NAME_DISPLAY);

    return configDef;
  }
}
