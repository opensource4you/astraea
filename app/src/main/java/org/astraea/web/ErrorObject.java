package org.astraea.web;

import java.util.NoSuchElementException;

class ErrorObject implements JsonObject {

  static ErrorObject for404(String message) {
    return new ErrorObject(404, message);
  }

  final int code;
  final String message;

  ErrorObject(Exception exception) {
    this(code(exception), exception.getMessage());
  }

  ErrorObject(int code, String message) {
    this.code = code;
    this.message = message;
  }

  private static int code(Exception exception) {
    if (exception instanceof IllegalArgumentException) return 400;
    if (exception instanceof NoSuchElementException) return 404;
    return 400;
  }
}
