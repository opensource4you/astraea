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
package org.astraea.app.metrics.collector;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.MBeanClient;

@FunctionalInterface
public interface Fetcher {
  /**
   * merge all fetchers into single one.
   *
   * @param fetchers cost function
   * @param errorHandler used to handle the runtime exception thrown by fetcher
   * @return fetcher if there is available fetcher. Otherwise, empty is returned
   */
  static Optional<Fetcher> of(
      Collection<Fetcher> fetchers, Consumer<RuntimeException> errorHandler) {
    if (fetchers.isEmpty()) return Optional.empty();
    return Optional.of(
        client ->
            fetchers.stream()
                .flatMap(
                    f -> {
                      try {
                        return f.fetch(client).stream();
                      } catch (RuntimeException e) {
                        errorHandler.accept(e);
                        return Stream.of();
                      }
                    })
                .collect(Collectors.toUnmodifiableList()));
  }

  /**
   * fetch to specify metrics from remote JMX server
   *
   * @param client mbean client (don't close it!)
   * @return java metrics
   */
  Collection<? extends HasBeanObject> fetch(MBeanClient client);
}
