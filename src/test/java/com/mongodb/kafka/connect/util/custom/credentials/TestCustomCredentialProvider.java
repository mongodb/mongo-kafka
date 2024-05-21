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
  public MongoCredential getCustomCredential(final Map<?, ?> configs) {
    return MongoCredential.createAwsCredential("userName", "password".toCharArray());
  }

  @Override
  public void validate(final Map<?, ?> configs) {
    String customProperty = (String) configs.get("customProperty");
    if (customProperty.equals("invalidValue")) {
      throw new ConfigException("Invalid value set for customProperty");
    }
  }

  @Override
  public void init(final Map<?, ?> configs) {
    customProperty = (String) configs.get("customProperty");
  }
}
