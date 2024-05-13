package com.mongodb.kafka.connect.util.custom.credentials;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.mongodb.MongoCredential;

public class TestCustomCredentialProvider implements CustomCredentialProvider {

  private String customProperty;

  public String getCustomProperty() {
    return customProperty;
  }

  @Override
  public MongoCredential getCustomCredential(Map<?, ?> configs) {
    return MongoCredential.createAwsCredential("userName", "password".toCharArray());
  }

  @Override
  public void validate(Map<?, ?> configs) {
    String customProperty = (String) configs.get("customProperty");
    if (customProperty.equals("invalidValue")) {
      throw new ConfigException("Invalid value set for customProperty");
    }
  }

  @Override
  public void init(Map<?, ?> configs) {
    customProperty = (String) configs.get("customProperty");
  }
}
