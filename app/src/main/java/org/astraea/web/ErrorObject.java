package org.astraea.web;

import java.util.NoSuchElementException;

class ErrorObject implements JsonObject {
  final int code;
  final String message;

  ErrorObject(Exception exception) {
    this.code = exception instanceof NoSuchElementException ? 404 : 400;
    this.message = exception.getMessage();
  }
}
