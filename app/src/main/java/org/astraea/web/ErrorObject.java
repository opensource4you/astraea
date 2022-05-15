package org.astraea.web;

import java.util.NoSuchElementException;

class ErrorObject implements JsonObject {

  static ErrorObject for404(String message) {
    return new ErrorObject(404, message);
  }

  final int code;
  final String message;

  ErrorObject(Exception exception) {
    this.code = exception instanceof NoSuchElementException ? 404 : 400;
    this.message = exception.getMessage();
  }

  ErrorObject(int code, String message) {
    this.code = code;
    this.message = message;
  }
}
