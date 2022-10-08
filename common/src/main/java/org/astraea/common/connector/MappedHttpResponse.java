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
package org.astraea.common.connector;

import java.net.URI;
import java.net.http.HttpClient.Version;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Optional;
import java.util.function.Function;
import javax.net.ssl.SSLSession;

/**
 * Convert HttpResponse body.
 *
 * @param <O> Old response body type
 * @param <T> New response body type
 */
public class MappedHttpResponse<O, T> implements HttpResponse<T> {

  private final HttpResponse<O> httpResponse;
  private final Function<O, T> mapped;
  private final T newBody;

  public MappedHttpResponse(HttpResponse<O> httpResponse, Function<O, T> mapped) {
    this.httpResponse = httpResponse;
    this.mapped = mapped;
    this.newBody = mapped.apply(httpResponse.body());
  }

  @Override
  public int statusCode() {
    return httpResponse.statusCode();
  }

  @Override
  public HttpRequest request() {
    return httpResponse.request();
  }

  @Override
  public Optional<HttpResponse<T>> previousResponse() {
    return httpResponse.previousResponse().map(x -> new MappedHttpResponse<>(x, mapped));
  }

  @Override
  public HttpHeaders headers() {
    return httpResponse.headers();
  }

  @Override
  public T body() {
    return newBody;
  }

  @Override
  public Optional<SSLSession> sslSession() {
    return httpResponse.sslSession();
  }

  @Override
  public URI uri() {
    return httpResponse.uri();
  }

  @Override
  public Version version() {
    return httpResponse.version();
  }
}
