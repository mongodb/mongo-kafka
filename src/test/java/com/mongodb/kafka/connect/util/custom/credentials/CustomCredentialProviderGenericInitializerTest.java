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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CustomCredentialProviderGenericInitializerTest {

  @Test
  @DisplayName("Test Exception scenarios")
  void testExceptions() {
    Map<String, Object> props = new HashMap<>();
    ConfigException configException =
        assertThrows(
            ConfigException.class,
            () -> CustomCredentialProviderGenericInitializer.initializeCustomProvider(props),
            "Expected initializeCustomProvider() to throw, but it didn't");
    assertEquals(
        CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG
            + " is not set to true. "
            + "CustomCredentialProvider should not be used.",
        configException.getMessage());
    props.put(CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG, true);
    configException =
        assertThrows(
            ConfigException.class,
            () -> CustomCredentialProviderGenericInitializer.initializeCustomProvider(props),
            "Expected initializeCustomProvider() to throw, but it didn't");
    assertEquals(
        CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS
            + " is required when "
            + CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG
            + " is set to true.",
        configException.getMessage());
    String qualifiedAuthProviderClassName = "com.nonexistant.package.Test";
    props.put(
        CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS,
        qualifiedAuthProviderClassName);
    configException =
        assertThrows(
            ConfigException.class,
            () -> CustomCredentialProviderGenericInitializer.initializeCustomProvider(props),
            "Expected initializeCustomProvider() to throw, but it didn't");
    assertEquals(
        "Unable to find " + qualifiedAuthProviderClassName + " on the classpath.",
        configException.getMessage());
    qualifiedAuthProviderClassName =
        "com.mongodb.kafka.connect.util.custom.credentials.TestInvalidCustomCredentialProvider";
    props.put(
        CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS,
        qualifiedAuthProviderClassName);
    configException =
        assertThrows(
            ConfigException.class,
            () -> CustomCredentialProviderGenericInitializer.initializeCustomProvider(props),
            "Expected initializeCustomProvider() to throw, but it didn't");
    assertEquals(
        "Provided Class does not implement CustomCredentialProvider interface.",
        configException.getMessage());
  }

  @Test
  @DisplayName("Test CustomCredentialProvider initialization")
  void testInitializeCustomCredentialProvider() {
    Map<String, Object> props = new HashMap<>();
    props.put(CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG, true);
    props.put(
        CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS,
        "com.mongodb.kafka.connect.util.custom.credentials.TestCustomCredentialProvider");
    props.put("customProperty", "customValue");
    CustomCredentialProvider customCredentialProvider =
        CustomCredentialProviderGenericInitializer.initializeCustomProvider(props);
    assertEquals(TestCustomCredentialProvider.class, customCredentialProvider.getClass());
  }

  @Test
  @DisplayName("Test CustomCredentialProvider custom props initialization")
  void testCustomPropsInit() {
    Map<String, Object> props = new HashMap<>();
    props.put(CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG, true);
    props.put(
        CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS,
        "com.mongodb.kafka.connect.util.custom.credentials.TestCustomCredentialProvider");
    props.put("customProperty", "customValue");
    TestCustomCredentialProvider customCredentialProvider =
        (TestCustomCredentialProvider)
            CustomCredentialProviderGenericInitializer.initializeCustomProvider(props);
    assertEquals("customValue", customCredentialProvider.getCustomProperty());
  }

  @Test
  @DisplayName("Test CustomCredentialProvider custom props validation")
  void testCustomPropsValidate() {
    Map<String, Object> props = new HashMap<>();
    props.put(CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG, true);
    props.put(
        CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS,
        "com.mongodb.kafka.connect.util.custom.credentials.TestCustomCredentialProvider");
    props.put("customProperty", "invalidValue");
    ConfigException configException =
        assertThrows(
            ConfigException.class,
            () -> CustomCredentialProviderGenericInitializer.initializeCustomProvider(props),
            "Expected initializeCustomProvider() to throw, but it didn't");
    assertEquals("Invalid value set for customProperty", configException.getMessage());
  }
}
