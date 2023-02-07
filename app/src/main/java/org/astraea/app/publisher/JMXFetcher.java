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
package org.astraea.app.publisher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.astraea.common.Utils;
import org.astraea.common.admin.Admin;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;

/** A pool of threads keep fetching MBeans of target brokerIds. */
public interface JMXFetcher {
  static List<JMXFetcher> create(
      int threads,
      String bootstrapServer,
      Function<Integer, Integer> idToJmxPort,
      BlockingQueue<Integer> targets,
      BlockingQueue<IdAndBean> idAndBeans) {
    var admin = Admin.of(bootstrapServer);
    Map<Integer, MBeanClient> idMBeanClient = new HashMap<>();
    var service = Executors.newFixedThreadPool(threads);
    var latches =
        IntStream.range(0, threads)
            .mapToObj(i -> new CountDownLatch(1))
            .collect(Collectors.toList());
    CompletableFuture.runAsync(
        () -> {
          try {
            latches.forEach(l -> Utils.swallowException(l::await));
          } finally {
            service.shutdown();
          }
        });

    return IntStream.range(0, threads)
        .mapToObj(
            i -> {
              var closed = new AtomicBoolean(false);
              var latch = latches.get(i);
              service.execute(
                  () -> {
                    while (!closed.get()) {
                      try {
                        var id = targets.poll(1, TimeUnit.SECONDS);
                        if (id == null) continue;
                        if (!idMBeanClient.containsKey(id)) {
                          // synchronize on creating(/updating) jmx connection
                          // TODO: The bootstrap server address may be different with jmx server
                          // address.
                          synchronized (idMBeanClient) {
                            if (!idMBeanClient.containsKey(id)) {
                              try {
                                admin
                                    .nodeInfos()
                                    .thenAccept(
                                        nodeInfos ->
                                            nodeInfos.stream()
                                                .filter(nodeInfo -> nodeInfo.id() == id)
                                                .findAny()
                                                .ifPresent(
                                                    nodeInfo ->
                                                        idMBeanClient.putIfAbsent(
                                                            id,
                                                            MBeanClient.jndi(
                                                                nodeInfo.host(),
                                                                idToJmxPort.apply(id)))))
                                    .toCompletableFuture()
                                    .get();
                              } catch (InterruptedException | ExecutionException e) {
                                // MBeanClient creation failed
                                e.printStackTrace();
                                continue;
                              }
                            }
                          }
                        }
                        idMBeanClient.get(id).beans(BeanQuery.all()).stream()
                            .map(bean -> new IdAndBean(id, bean))
                            .forEach(idAndBeans::add);
                      } catch (InterruptedException ie) {
                        // Blocking queue take() interrupted, print message and ignore.
                        ie.printStackTrace();
                      }
                    }
                    latch.countDown();
                  });
              return (JMXFetcher) () -> closed.set(true);
            })
        .collect(Collectors.toList());
  }

  void close();

  class IdAndBean {
    private final int id;
    private final BeanObject bean;

    public IdAndBean(int id, BeanObject bean) {
      this.id = id;
      this.bean = bean;
    }

    public int id() {
      return this.id;
    }

    public BeanObject bean() {
      return bean;
    }
  }
}
