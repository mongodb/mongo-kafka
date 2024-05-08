package com.mongodb.kafka.connect.util.custom.credentials;

import java.util.Map;

import com.mongodb.MongoCredential;

public interface CustomCredentialProvider {
  MongoCredential getCustomCredential(Map<?, ?> configs);

  void validate(Map<?, ?> configs);

  void init(Map<?, ?> configs);
}
