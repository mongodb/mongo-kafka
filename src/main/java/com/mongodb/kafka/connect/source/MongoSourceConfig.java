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

import static com.mongodb.kafka.connect.util.ClassHelper.createInstance;
import static com.mongodb.kafka.connect.util.ConfigHelper.collationFromJson;
import static com.mongodb.kafka.connect.util.ConfigHelper.fullDocumentFromString;
import static com.mongodb.kafka.connect.util.ConfigHelper.jsonArrayFromString;
import static com.mongodb.kafka.connect.util.Validators.emptyString;
import static com.mongodb.kafka.connect.util.Validators.errorCheckingValueValidator;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.config.ConfigDef.Width;

import java.util.List;
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
  private static final String OUTPUT_FORMAT_KEY_DEFAULT = OutputFormat.JSON.name().toLowerCase();
  private static final String OUTPUT_FORMAT_KEY_DISPLAY = "The key output format";
  private static final String OUTPUT_FORMAT_KEY_DOC =
      "The output format of the data produced by the connector for the key. Supported formats are:\n"
          + " * `json` - Raw Json strings\n"
          + " * `bson` - Bson byte array\n";

  public static final String OUTPUT_FORMAT_VALUE_CONFIG = "output.format.value";
  private static final String OUTPUT_FORMAT_VALUE_DEFAULT = OutputFormat.JSON.name().toLowerCase();
  private static final String OUTPUT_FORMAT_VALUE_DISPLAY = "The value output format";
  private static final String OUTPUT_FORMAT_VALUE_DOC =
      "The output format of the data produced by the connector for the value. Supported formats are:\n"
          + " * `json` - Raw Json strings\n"
          + " * `bson` - Bson byte array\n";

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

  public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
  private static final String TOPIC_PREFIX_DOC =
      "Prefix to prepend to database & collection names to generate the name of the Kafka "
          + "topic to publish data to.";
  private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";
  private static final String TOPIC_PREFIX_DEFAULT = "";

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

  public static final ConfigDef CONFIG = createConfigDef();
  private static final List<Consumer<MongoSourceConfig>> INITIALIZERS =
      singletonList(MongoSourceConfig::validateCollection);

  public enum OutputFormat {
    JSON,
    BSON
  }

  private final ConnectionString connectionString;
  private JsonWriterSettings jsonWriterSettings;

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
    return jsonArrayFromString(getString(PIPELINE_CONFIG));
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

  public JsonWriterSettings getJsonWriterSettings() {
    if (jsonWriterSettings == null) {
      jsonWriterSettings =
          createInstance(
                  OUTPUT_JSON_FORMATTER_CONFIG,
                  getString(OUTPUT_JSON_FORMATTER_CONFIG),
                  JsonWriterSettingsProvider.class)
              .getJsonWriterSettings();
    }
    return jsonWriterSettings;
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

    return configDef;
  }
}
