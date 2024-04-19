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

import static com.mongodb.kafka.connect.source.MongoSourceConfig.COLLECTION_CONFIG;
import static com.mongodb.kafka.connect.source.MongoSourceConfig.DATABASE_CONFIG;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonBinaryReader;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;

import com.mongodb.kafka.connect.source.MongoSourceConfig.StartupConfig.CopyExistingConfig;

/**
 * Copy Data Manager
 *
 * <ol>
 * <li>Gets all namespaces to copy. eg. A single collection, all collections in
 * a database or all
 * collections in all databases.
 * <li>For each namespace, submit to the executors an copy existing task.
 * <li>Each copy existing task, runs an aggregation pipeline on the namespace,
 * to mimic an insert
 * document and adds the results to the queue.
 * <li>The poll method returns documents from the queue.
 * </ol>
 */
class MongoCopyDataManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoCopyDataManager.class);
  private static final String NAMESPACE_FIELD = "ns";
  static final String ALT_NAMESPACE_FIELD = "__";
  private static final byte[] NAMESPACE_BYTES = NAMESPACE_FIELD.getBytes(StandardCharsets.UTF_8);

  private static final String PIPELINE_TEMPLATE = format(
      "{$replaceRoot: "
          + "{newRoot: {"
          + "_id: {_id: '$_id', copyingData: true}, "
          + "operationType: 'insert', "
          + "%s: {db: '%%s', coll: '%%s'}"
          + "documentKey: {_id: '$_id'}, "
          + "fullDocument: '$$ROOT'}}"
          + "}",
      NAMESPACE_FIELD);

  private static final BsonDocument ADD_ALT_NAMESPACE_STAGE = BsonDocument.parse(
      format("{'$addFields': {'%s': '$%s'}}", ALT_NAMESPACE_FIELD, NAMESPACE_FIELD));
  private static final BsonDocument UNSET_ORIGINAL_NAMESPACE_STAGE = BsonDocument
      .parse(format("{'$project': {'%s': 0}}", NAMESPACE_FIELD));

  private volatile boolean closed;
  private volatile Exception errorException;
  private final AtomicInteger namespacesToCopy;
  private final MongoSourceConfig sourceConfig;
  private final MongoClient mongoClient;
  private final ExecutorService executor;
  private final ArrayBlockingQueue<BsonDocument> queue;

  MongoCopyDataManager(final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {
    this.sourceConfig = sourceConfig;
    this.mongoClient = mongoClient;

    List<MongoNamespace> namespaces = selectNamespaces(sourceConfig, mongoClient);

    LOGGER.info("Copying existing data on the following namespaces: {}", namespaces);
    namespacesToCopy = new AtomicInteger(namespaces.size());
    CopyExistingConfig copyConfig = sourceConfig.getStartupConfig().copyExistingConfig();
    queue = new ArrayBlockingQueue<>(copyConfig.queueSize());
    executor = Executors.newFixedThreadPool(
        Math.max(1, Math.min(namespaces.size(), copyConfig.maxThreads())));
    namespaces.forEach(n -> executor.submit(() -> copyDataFrom(n)));
  }

  Optional<BsonDocument> poll() {
    if (errorException != null) {
      if (!closed) {
        close();
      }
      throw new ConnectException(errorException);
    }

    if (namespacesToCopy.get() == 0) {
      close();
    }
    return Optional.ofNullable(queue.poll());
  }

  boolean isCopying() {
    return namespacesToCopy.get() > 0 || !queue.isEmpty();
  }

  @Override
  public void close() {
    if (!closed) {
      closed = true;
      LOGGER.info("Shutting down copy data manager executors");
      executor.shutdownNow();
    }
  }

  private void copyDataFrom(final MongoNamespace namespace) {
    LOGGER.info(
        "Copying existing data started from: {}", namespace.getFullName());
    try {
      AtomicLong totalGetFromSource = new AtomicLong();
      mongoClient
          .getDatabase(namespace.getDatabaseName())
          .getCollection(namespace.getCollectionName(), RawBsonDocument.class)
          .aggregate(createPipelineV2(sourceConfig, namespace))
          .allowDiskUse(sourceConfig.getStartupConfig().copyExistingConfig().allowDiskUse())
          .forEach(
              (a) -> {
                totalGetFromSource.getAndIncrement();
                this.putToQueueV2(a, namespace);
              });
      LOGGER.info(
          "Copying existing data Completed from: {} TotalCopied: {}",
          namespace.getFullName(),
          totalGetFromSource.get());
      namespacesToCopy.decrementAndGet();
    } catch (Exception e) {
      errorException = e;
    }
  }

  private void putToQueue(final RawBsonDocument bsonDocument) {
    try {
      queue.put(convertDocument(bsonDocument));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static List<MongoNamespace> selectNamespaces(
      final MongoSourceConfig sourceConfig, final MongoClient mongoClient) {

    String database = sourceConfig.getString(DATABASE_CONFIG);
    String collection = sourceConfig.getString(COLLECTION_CONFIG);
    String namespacesRegex = sourceConfig.getStartupConfig().copyExistingConfig().namespaceRegex();

    List<MongoNamespace> namespaces;
    if (database.isEmpty()) {
      namespaces = getCollections(mongoClient);
    } else if (collection.isEmpty()) {
      namespaces = getCollections(mongoClient, database);
    } else {
      namespaces = singletonList(createNamespace(database, collection));
    }

    if (!namespacesRegex.isEmpty()) {
      Predicate<String> predicate = Pattern.compile(namespacesRegex).asPredicate();
      namespaces = namespaces.stream().filter(n -> predicate.test(n.getFullName())).collect(toList());
    }

    return namespaces;
  }

  static List<Bson> createPipeline(final MongoSourceConfig cfg, final MongoNamespace namespace) {
    List<Bson> pipeline = new ArrayList<>();
    cfg.getStartupConfig().copyExistingConfig().pipeline().map(pipeline::addAll);
    pipeline.add(
        BsonDocument.parse(
            format(PIPELINE_TEMPLATE, namespace.getDatabaseName(), namespace.getCollectionName())));
    cfg.getPipeline().map(pipeline::addAll);
    pipeline.add(ADD_ALT_NAMESPACE_STAGE);
    pipeline.add(UNSET_ORIGINAL_NAMESPACE_STAGE);
    return pipeline;
  }

  static List<Bson> createPipelineV2(MongoSourceConfig cfg, MongoNamespace namespace) {
    List<Bson> pipeline = new ArrayList<>();
    Optional<List<Document>> tempPipeline;
    if (cfg.getStartupConfig().copyExistingConfig().pipeline().isPresent()) {
      tempPipeline = cfg.getStartupConfig().copyExistingConfig().pipeline();
      tempPipeline.map(pipeline::addAll);
    }

    LOGGER.info("Copy exist MongoDB Pipeline Start for namespace: {}", namespace.getFullName());
    pipeline.forEach((a) -> {
          LOGGER.info("{}", a.toBsonDocument().toJson());
        });
    LOGGER.info(
        "Copy exist MongoDB Pipeline End {}",
        namespace.getFullName(),
        namespace.getCollectionName());
    return pipeline;
  }

  static RawBsonDocument convertDocument(final RawBsonDocument original) {
    ByteBuffer sourceBuffer = original.getByteBuffer().asNIO();
    BsonBinaryReader reader = new BsonBinaryReader(sourceBuffer);
    int currentPosition = 0;
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      if (reader.readName().equals(ALT_NAMESPACE_FIELD)) {
        currentPosition++; // Adjust the current position to include the bson type
        byte[] sourceBytes = sourceBuffer.array();
        // Convert the namespace field in situ
        for (byte namespaceByte : NAMESPACE_BYTES) {
          sourceBytes[currentPosition++] = namespaceByte;
        }
        return original;
      }
      reader.skipValue();
      currentPosition = reader.getBsonInput().getPosition();
    }
    return original;
  }

  static BsonDocument convertDocumentV2(BsonDocument original, MongoNamespace namespace) {
    original = convertToChangeStreamDocument(original, namespace);
    return original;
  }

  public static BsonDocument convertToChangeStreamDocument(
      BsonDocument original, MongoNamespace namespace) {
    BsonDocument output = new BsonDocument();
    BsonValue id = original.get("_id", new BsonObjectId());
    output.append(
        "_id", (new BsonDocument()).append("_id", id).append("copyingData", new BsonBoolean(true)));
    output.append("operationType", new BsonString("insert"));
    output.append("documentKey", (new BsonDocument()).append("_id", id));
    output.append("fullDocument", original);
    BsonDocument doubleUnderscore = new BsonDocument();
    doubleUnderscore.append("db", new BsonString(namespace.getDatabaseName()));
    doubleUnderscore.append("coll", new BsonString(namespace.getCollectionName()));
    output.append("ns", doubleUnderscore);
    return output;
  }

  private void putToQueueV2(BsonDocument bsonDocument, MongoNamespace namespace) {
    try {
      this.queue.put(convertDocumentV2(bsonDocument, namespace));
    } catch (InterruptedException var4) {
      throw new RuntimeException(var4);
    }
  }

  private static List<MongoNamespace> getCollections(final MongoClient mongoClient) {
    return mongoClient.listDatabaseNames().into(new ArrayList<>()).stream()
        .filter(s -> !(s.startsWith("admin") || s.startsWith("config") || s.startsWith("local")))
        .map(d -> getCollections(mongoClient, d))
        .flatMap(Collection::stream)
        .collect(toList());
  }

  private static List<MongoNamespace> getCollections(
      final MongoClient mongoClient, final String database) {
    return mongoClient.getDatabase(database).listCollectionNames().into(new ArrayList<>()).stream()
        .filter(s -> !s.startsWith("system."))
        .map(c -> createNamespace(database, c))
        .collect(toList());
  }

  private static MongoNamespace createNamespace(final String database, final String collection) {
    return new MongoNamespace(database, collection);
  }
}
