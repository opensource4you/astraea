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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.KafkaMetrics;
import org.astraea.app.metrics.jmx.MBeanClient;

public class BeanCollector {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private BiFunction<String, Integer, MBeanClient> clientCreator = MBeanClient::jndi;
    private Duration interval = Duration.ofSeconds(3);
    private int numberOfObjectsPerNode = 300;

    private Builder() {}

    public Builder clientCreator(BiFunction<String, Integer, MBeanClient> clientCreator) {
      this.clientCreator = Objects.requireNonNull(clientCreator);
      return this;
    }

    public Builder interval(Duration interval) {
      this.interval = Objects.requireNonNull(interval);
      return this;
    }

    public Builder numberOfObjectsPerNode(int numberOfObjectsPerNode) {
      this.numberOfObjectsPerNode = Utils.requirePositive(numberOfObjectsPerNode);
      return this;
    }

    public BeanCollector build() {
      return new BeanCollector(clientCreator, interval, numberOfObjectsPerNode);
    }
  }

  private final BiFunction<String, Integer, MBeanClient> clientCreator;
  private final Duration interval;
  private final int numberOfObjectsPerNode;

  // visible for testing
  final ConcurrentMap<String, Node> nodes = new ConcurrentSkipListMap<>();

  private BeanCollector(
      BiFunction<String, Integer, MBeanClient> clientCreator,
      Duration interval,
      int numberOfObjectsPerNode) {
    this.clientCreator = clientCreator;
    this.interval = interval;
    this.numberOfObjectsPerNode = numberOfObjectsPerNode;
  }

  public Register register() {
    return new Register() {
      private boolean local = false;
      private String host;
      private int port = -1;
      private Fetcher fetcher = client -> List.of(KafkaMetrics.Host.jvmMemory(client));

      @Override
      public Register host(String host) {
        this.host = Objects.requireNonNull(host);
        return this;
      }

      @Override
      public Register port(int port) {
        this.port = Utils.requirePositive(port);
        return this;
      }

      @Override
      public Register local() {
        this.local = true;
        this.port = -1;
        this.host = Utils.hostname();
        return this;
      }

      @Override
      public Register fetcher(Fetcher fetcher) {
        this.fetcher = Objects.requireNonNull(fetcher);
        return this;
      }

      @Override
      public Receiver build() {
        var nodeKey = host + ":" + port;
        var node = nodes.computeIfAbsent(nodeKey, ignored -> new Node());
        var receiver =
            new Receiver() {
              private final Map<Long, HasBeanObject> objects = new ConcurrentSkipListMap<>();

              @Override
              public String host() {
                return host;
              }

              @Override
              public int port() {
                return port;
              }

              @Override
              public Collection<HasBeanObject> current() {
                tryUpdate();
                return Collections.unmodifiableCollection(objects.values());
              }

              @Override
              public void close() {
                node.lock.lock();
                try {
                  node.receivers.remove(this);
                  if (node.receivers.isEmpty() && node.mBeanClient != null)
                    Utils.swallowException(node.mBeanClient::close);
                  node.mBeanClient = null;
                } finally {
                  node.lock.unlock();
                }
              }

              private synchronized void tryUpdate() {
                var needUpdate =
                    objects.keySet().stream()
                        .max((Long::compare))
                        .map(last -> last + interval.toMillis() <= System.currentTimeMillis())
                        .orElse(true);
                if (needUpdate && node.lock.tryLock()) {
                  try {
                    if (node.mBeanClient == null)
                      node.mBeanClient =
                          local ? MBeanClient.local() : clientCreator.apply(host, port);
                    var beans = fetcher.fetch(node.mBeanClient);
                    // remove old beans if the queue is full
                    for (var t : objects.keySet()) {
                      if (objects.size() + beans.size() <= numberOfObjectsPerNode) break;
                      objects.remove(t);
                    }
                    long now =
                        beans.stream()
                            .mapToLong(HasBeanObject::createdTimestamp)
                            .min()
                            .orElse(System.currentTimeMillis());
                    for (var bean : beans) objects.put(now++, bean);
                  } finally {
                    node.lock.unlock();
                  }
                }
              }
            };

        // add receiver
        node.lock.lock();
        try {
          node.receivers.add(receiver);
        } finally {
          node.lock.unlock();
        }
        return receiver;
      }
    };
  }

  // visible for testing
  static final class Node {
    private final Set<Receiver> receivers = new HashSet<>();
    private final Lock lock = new ReentrantLock();
    // visible for testing
    MBeanClient mBeanClient;
  }
}
