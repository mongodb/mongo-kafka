package com.mongodb.kafka.connect.util.custom.credentials;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCredentialProviderGenericInitializer {

  static final Logger LOGGER =
      LoggerFactory.getLogger(CustomCredentialProviderGenericInitializer.class);

  public static CustomCredentialProvider initializeCustomProvider(Map<?, ?> originals)
      throws ConfigException {
    // Validate if CUSTOM_AUTH_ENABLE_CONFIG is set to true
    String customAuthMechanismEnabled =
        String.valueOf(originals.get(CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG));
    if (customAuthMechanismEnabled == null
        || customAuthMechanismEnabled.equals("null")
        || customAuthMechanismEnabled.isEmpty()) {
      throw new ConfigException(
          CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG
              + " is not set to true. "
              + "CustomCredentialProvider should not be used.");
    }
    // Validate if CUSTOM_AUTH_PROVIDER_CLASS is provided
    String qualifiedAuthProviderClassName =
        String.valueOf(originals.get(CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS));
    if (qualifiedAuthProviderClassName == null
        || qualifiedAuthProviderClassName.equals("null")
        || qualifiedAuthProviderClassName.isEmpty()) {
      throw new ConfigException(
          CustomCredentialProviderConstants.CUSTOM_AUTH_PROVIDER_CLASS
              + " is required when "
              + CustomCredentialProviderConstants.CUSTOM_AUTH_ENABLE_CONFIG
              + " is set to true.");
    }
    try {
      // Validate if qualifiedAuthProviderClassName is on the class path.
      Class<?> authProviderClass =
          Class.forName(
              qualifiedAuthProviderClassName,
              false,
              CustomCredentialProviderGenericInitializer.class.getClassLoader());
      // Validate if qualifiedAuthProviderClassName implements CustomCredentialProvider interface.
      if (!CustomCredentialProvider.class.isAssignableFrom(authProviderClass)) {
        throw new ConfigException(
            "Provided Class does not implement CustomCredentialProvider interface.");
      }
      CustomCredentialProvider customCredentialProvider =
          initializeCustomProvider(authProviderClass);
      // Perform config validations specific to CustomCredentialProvider impl provided
      customCredentialProvider.validate(originals);
      // Initialize custom variables required by implementation of CustomCredentialProvider
      customCredentialProvider.init(originals);
      return customCredentialProvider;
    } catch (ClassNotFoundException e) {
      throw new ConfigException(
          "Unable to find " + qualifiedAuthProviderClassName + " on the classpath.");
    }
  }

  private static CustomCredentialProvider initializeCustomProvider(Class<?> authProviderClass) {
    try {
      return (CustomCredentialProvider) authProviderClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      LOGGER.error("Error while instantiating " + authProviderClass + " class");
      throw new RuntimeException(e);
    }
  }
}
