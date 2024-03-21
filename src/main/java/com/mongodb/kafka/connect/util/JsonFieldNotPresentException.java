package com.mongodb.kafka.connect.util;

public class JsonFieldNotPresentException extends Exception {

  public JsonFieldNotPresentException() {}

  public JsonFieldNotPresentException(final String message) {
    super(message);
  }
}
