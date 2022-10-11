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
package org.astraea.gui;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.Broker;
import org.astraea.common.metrics.MBeanClient;

public class Context {
  private final AtomicReference<AsyncAdmin> asyncAdminReference = new AtomicReference<>();
  private final AtomicInteger jmxPort = new AtomicInteger(-1);

  public Optional<AsyncAdmin> replace(AsyncAdmin admin) {
    return Optional.ofNullable(asyncAdminReference.getAndSet(admin));
  }

  public void replace(int jmxPort) {
    this.jmxPort.set(jmxPort);
  }

  public <T> T submit(Function<AsyncAdmin, T> executor) {
    var admin = asyncAdminReference.get();
    if (admin == null) throw new IllegalArgumentException("Please define bootstrap servers");
    return executor.apply(admin);
  }

  public void execute(Consumer<AsyncAdmin> executor) {
    var admin = asyncAdminReference.get();
    if (admin == null) throw new IllegalArgumentException("Please define bootstrap servers");
    executor.accept(admin);
  }

  public <T> CompletionStage<T> metrics(Function<Map<Broker, MBeanClient>, T> executor) {
    var jmxPort = this.jmxPort.get();
    if (jmxPort < 0) throw new IllegalArgumentException("Please define jmxPort");
    return submit(
            admin ->
                admin
                    .brokers()
                    .thenApply(
                        brokers ->
                            brokers.stream()
                                .collect(
                                    Collectors.toMap(
                                        b -> b, b -> MBeanClient.jndi(b.host(), jmxPort)))))
        .thenApply(executor);
  }

  public boolean hasMetrics() {
    return jmxPort.get() > 0;
  }
}
