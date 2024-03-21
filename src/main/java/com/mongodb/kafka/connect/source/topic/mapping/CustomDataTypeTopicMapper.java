package com.mongodb.kafka.connect.source.topic.mapping;

import static com.mongodb.kafka.connect.source.MongoSourceConfig.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.bson.BsonDocument;

import com.mongodb.kafka.connect.source.MongoSourceConfig;
import com.mongodb.kafka.connect.util.JsonFieldNotPresentException;

public class CustomDataTypeTopicMapper implements TopicMapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(CustomDataTypeTopicMapper.class);

  public static final String OPERATION_TYPE_FIELD = "operationType";
  public static final String FULL_DOCUMENT_FIELD = "fullDocument";
  public static final String DATA_TYPE_FIELD = "dataType";

  private String separator;
  private String prefix;
  private String suffix;
  private String database;
  private String collection;
  private String dlqTopicName;

  @Override
  public void configure(final MongoSourceConfig config) {
    final String prefix = config.getString(TOPIC_PREFIX_CONFIG);
    final String suffix = config.getString(TOPIC_SUFFIX_CONFIG);

    this.separator = config.getString(TOPIC_SEPARATOR_CONFIG);
    this.prefix = prefix.isEmpty() ? prefix : prefix + separator;
    this.suffix = suffix.isEmpty() ? suffix : separator + suffix;
    this.database = config.getString(DATABASE_CONFIG);
    this.collection = config.getString(COLLECTION_CONFIG);
    this.dlqTopicName = config.getString(OVERRIDE_ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG);
  }

  @Override
  public String getTopic(final BsonDocument changeStreamDocument) {
    try {
      validateJsonFields(changeStreamDocument);
    } catch (JsonFieldNotPresentException e) {
      LOGGER.error("Exception occurred while trying to validate JSON fields", e);
      return dlqTopicName;
    }

    try {
      final BsonDocument fullDocument = changeStreamDocument.get(FULL_DOCUMENT_FIELD).asDocument();

      if (!fullDocument.containsKey(DATA_TYPE_FIELD)) {
        return String.join("", prefix, database, separator, collection, suffix);
      }

      final String dataType = fullDocument.get(DATA_TYPE_FIELD).asString().getValue();

      return String.join("", prefix, database, separator, collection, separator, dataType, suffix);

    } catch (Exception e) {
      LOGGER.error("Exception occurred while trying to get topic name", e);
      return dlqTopicName;
    }
  }

  private void validateJsonFields(final BsonDocument changeStreamDocument)
      throws JsonFieldNotPresentException {
    if (!changeStreamDocument.containsKey(OPERATION_TYPE_FIELD)) {
      throw new JsonFieldNotPresentException(OPERATION_TYPE_FIELD + " not present");
    }

    if (!changeStreamDocument.containsKey(FULL_DOCUMENT_FIELD)) {
      throw new JsonFieldNotPresentException(FULL_DOCUMENT_FIELD + " not present");
    }
  }
}
