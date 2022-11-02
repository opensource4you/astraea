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
package org.astraea.common.http;

import java.lang.reflect.Type;
import java.net.http.HttpResponse;

public class StringResponseException extends RuntimeException {

  private final HttpResponse<String> httpResponse;

  public StringResponseException(HttpResponse<String> httpResponse) {
    super(
        String.format("Failed response: %s, %s.", httpResponse.statusCode(), httpResponse.body()));
    this.httpResponse = httpResponse;
  }

  public StringResponseException(HttpResponse<String> httpResponse, Type type, Exception cause) {
    super(
        String.format("Response json `%s` can't convert to Object %s.", httpResponse.body(), type),
        cause);
    this.httpResponse = httpResponse;
  }

  public int statusCode() {
    return httpResponse.statusCode();
  }

  public String body() {
    return httpResponse.body();
  }
}
