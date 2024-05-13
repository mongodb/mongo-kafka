package com.mongodb.kafka.connect.util.custom.credentials;

public final class CustomCredentialProviderConstants {
  private CustomCredentialProviderConstants() {}

  public static final String CUSTOM_AUTH_ENABLE_CONFIG = "mongo.custom.auth.mechanism.enable";

  public static final String CUSTOM_AUTH_PROVIDER_CLASS =
      "mongo.custom.auth.mechanism.providerClass";
}
