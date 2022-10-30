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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

interface Handler {

  static <T> Set<T> compare(Set<T> all, Optional<T> target) {
    var nonexistent = target.stream().filter(id -> !all.contains(id)).collect(Collectors.toSet());
    if (!nonexistent.isEmpty()) throw new NoSuchElementException(nonexistent + " does not exist");
    return target.map(Set::of).orElse(all);
  }

  default CompletionStage<? extends Response> process(Channel channel) {
    var start = System.currentTimeMillis();
    try {
      switch (channel.type()) {
        case GET:
          return get(channel);
        case POST:
          return post(channel);
        case DELETE:
          return delete(channel);
        case PUT:
          return put(channel);
        default:
          return CompletableFuture.completedFuture(Response.NOT_FOUND);
      }
    } catch (Exception e) {
      e.printStackTrace();
      return CompletableFuture.completedFuture(Response.of(e));
    } finally {
      System.out.println(
          "take "
              + (System.currentTimeMillis() - start)
              + "ms to process request from "
              + channel.type().name()
              + " on "
              + channel.target());
    }
  }

  default CompletionStage<? extends Response> handle(Channel channel) {
    var response = process(channel);
    response.whenComplete(
        (r, e) -> {
          if (e != null) channel.send(Response.of(e));
          else channel.send(r);
        });
    return response;
  }

  /**
   * handle the get request.
   *
   * @return json object to return
   */
  CompletionStage<? extends Response> get(Channel channel);

  /**
   * handle the post request.
   *
   * @return json object to return
   */
  default CompletionStage<? extends Response> post(Channel channel) {
    return CompletableFuture.completedFuture(Response.NOT_FOUND);
  }

  /**
   * handle the delete request.
   *
   * @return json object to return
   */
  default CompletionStage<? extends Response> delete(Channel channel) {
    return CompletableFuture.completedFuture(Response.NOT_FOUND);
  }

  /**
   * handle the put request.
   *
   * @return json object to return
   */
  default CompletionStage<? extends Response> put(Channel channel) {
    return CompletableFuture.completedFuture(Response.NOT_FOUND);
  }
}
