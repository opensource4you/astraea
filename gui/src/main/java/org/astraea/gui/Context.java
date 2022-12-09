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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javafx.stage.Stage;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.metrics.MBeanClient;

public class Context {
  private final AtomicReference<Admin> adminReference = new AtomicReference<>();

  private final AtomicReference<ConnectorClient> connectorClientReference = new AtomicReference<>();

  private Stage stage;
  private volatile int jmxPort = -1;
  private final Map<Integer, MBeanClient> clients = new ConcurrentHashMap<>();

  public Context() {}

  public Context(Admin admin) {
    adminReference.set(admin);
  }

  public void stage(Stage stage) {
    this.stage = stage;
  }

  public Stage stage() {
    return Objects.requireNonNull(stage);
  }

  public void replace(Admin admin) {
    var previous = adminReference.getAndSet(admin);
    if (previous != null) previous.close();
  }

  public void replace(ConnectorClient connectorClient) {
    connectorClientReference.getAndSet(connectorClient);
  }

  public void replace(List<NodeInfo> nodes, int jmxPort) {
    var copy = Map.copyOf(this.clients);
    this.jmxPort = jmxPort;
    this.clients.clear();
    this.clients.putAll(
        nodes.stream()
            .collect(Collectors.toMap(NodeInfo::id, n -> MBeanClient.jndi(n.host(), jmxPort))));
    copy.values().forEach(MBeanClient::close);
  }

  public Map<Integer, MBeanClient> clients(List<NodeInfo> nodeInfos) {
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
              var previous = clients.put(n.id(), MBeanClient.jndi(n.host(), jmxPort));
              if (previous != null) previous.close();
            });
    return Map.copyOf(clients);
  }

  public Admin admin() {
    var admin = adminReference.get();
    if (admin == null) throw new IllegalArgumentException("Please define bootstrap servers");
    return admin;
  }

  public ConnectorClient connectorClient() {
    var connectorClient = connectorClientReference.get();
    if (connectorClient == null) throw new IllegalArgumentException("Please define worker urls");
    return connectorClient;
  }

  public Map<Integer, MBeanClient> clients() {
    var copy = Map.copyOf(clients);
    if (copy.isEmpty()) throw new IllegalArgumentException("Please define jmx port");
    return copy;
  }

  public boolean hasMetrics() {
    return !clients.isEmpty();
  }
}
