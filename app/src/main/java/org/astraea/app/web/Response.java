/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.app.web;

import java.util.NoSuchElementException;
import org.astraea.common.json.JsonConverter;

interface Response {

  Response OK = new ResponseImpl(200);
  Response ACCEPT = new ResponseImpl(202);
  Response BAD_REQUEST = new ResponseImpl(400);
  Response NOT_FOUND = new ResponseImpl(404);

  static Response of(Throwable exception) {
    return new ResponseImpl(code(exception), exception.getMessage());
  }

  static Response for404(String message) {
    return new ResponseImpl(404, message);
  }

  static Response for500(String message) {
    return new ResponseImpl(500, message);
  }

  private static int code(Throwable exception) {
    if (exception instanceof IllegalArgumentException) return 400;
    if (exception instanceof NoSuchElementException) return 404;
    return 400;
  }

  /** @return http code */
  default int code() {
    return 200;
  }

  default String json() {
    return JsonConverter.defaultConverter().toJson(this);
  }

  /**
   * Callback fired when the response is sent, regardless of success or failure.
   *
   * @param error that cause sending response failed, will be null when sending response is
   *     successful.
   */
  default void onComplete(Throwable error) {}

  class ResponseImpl implements Response {
    final int code;
    final String message;

    ResponseImpl(int code) {
      this(code, "");
    }

    ResponseImpl(int code, String message) {
      this.code = code;
      this.message = message;
    }

    @Override
    public int code() {
      return code;
    }

    @Override
    public String json() {
      return message == null || message.isEmpty() ? "" : JsonConverter.defaultConverter().toJson(this);
    }
  }
}
