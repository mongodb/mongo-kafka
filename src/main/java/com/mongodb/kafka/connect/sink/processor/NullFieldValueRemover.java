package com.mongodb.kafka.connect.sink.processor;

import org.apache.kafka.connect.sink.SinkRecord;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;

public class NullFieldValueRemover extends PostProcessor {

  public NullFieldValueRemover(MongoSinkTopicConfig config) {
    super(config);
  }

  @Override
  public void process(SinkDocument doc, SinkRecord orig) {
    doc.getValueDoc().ifPresent(this::removeNullFieldValues);
  }

  private void removeNullFieldValues(final BsonDocument doc) {
    doc.entrySet()
        .removeIf(
            entry -> {
              BsonValue value = entry.getValue();
              if (value.isDocument()) {
                removeNullFieldValues(value.asDocument());
              }
              return value.isNull();
            });
  }
}
