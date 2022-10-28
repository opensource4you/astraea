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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.astraea.common.admin.AsyncAdmin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.metrics.MBeanClient;

public class Context {
  private final AtomicReference<AsyncAdmin> asyncAdminReference = new AtomicReference<>();

  private volatile int jmxPort = -1;
  private final Map<NodeInfo, MBeanClient> clients = new ConcurrentHashMap<>();

  public Context() {}

  public Context(AsyncAdmin admin) {
    asyncAdminReference.set(admin);
  }

  public void replace(AsyncAdmin admin) {
    var previous = asyncAdminReference.getAndSet(admin);
    if (previous != null) previous.close();
  }

  public void replace(Set<NodeInfo> nodes, int jmxPort) {
    var copy = Map.copyOf(this.clients);
    this.jmxPort = jmxPort;
    this.clients.clear();
    this.clients.putAll(
        nodes.stream().collect(Collectors.toMap(n -> n, n -> MBeanClient.jndi(n.host(), jmxPort))));
    copy.values().forEach(MBeanClient::close);
  }

  public Map<NodeInfo, MBeanClient> clients(Set<NodeInfo> nodeInfos) {
    if (jmxPort < 0) throw new IllegalArgumentException("Please define jmxPort");
    clients.keySet().stream()
        .filter(n -> !nodeInfos.contains(n))
        .collect(Collectors.toList())
        .forEach(
            n -> {
              var previous = clients.remove(n);
              if (previous != null) previous.close();
            });
    nodeInfos.stream()
        .filter(n -> !clients.containsKey(n))
        .forEach(
            n -> {
              var previous = clients.put(n, MBeanClient.jndi(n.host(), jmxPort));
              if (previous != null) previous.close();
            });
    return Map.copyOf(clients);
  }

  public AsyncAdmin admin() {
    var admin = asyncAdminReference.get();
    if (admin == null) throw new IllegalArgumentException("Please define bootstrap servers");
    return admin;
  }

  public Map<NodeInfo, MBeanClient> clients() {
    var copy = Map.copyOf(clients);
    if (copy.isEmpty()) throw new IllegalArgumentException("Please define jmx port");
    return copy;
  }

  public boolean hasMetrics() {
    return !clients.isEmpty();
  }
}
