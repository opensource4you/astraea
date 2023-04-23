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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import javafx.stage.Stage;
import org.astraea.common.admin.Admin;
import org.astraea.common.admin.NodeInfo;
import org.astraea.common.connector.ConnectorClient;
import org.astraea.common.metrics.MBeanClient;

public class Context {
  private final AtomicReference<Admin> adminReference = new AtomicReference<>();

  private final AtomicReference<ConnectorClient> connectorClientReference = new AtomicReference<>();

  private Stage stage;

  private volatile Clients<Integer> brokerClients;

  private volatile Clients<String> workerClients;

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

  public void brokerJmxPort(int brokerJmxPort) {
    if (brokerClients != null) brokerClients.clients.values().forEach(MBeanClient::close);
    brokerClients = new Clients<>(brokerJmxPort);
  }

  public void workerJmxPort(int workerJmxPort) {
    if (workerClients != null) workerClients.clients.values().forEach(MBeanClient::close);
    workerClients = new Clients<>(workerJmxPort);
  }

  @SuppressWarnings("resource")
  public Map<Integer, MBeanClient> addBrokerClients(List<NodeInfo> nodeInfos) {
    if (brokerClients == null) return Map.of();
    nodeInfos.forEach(
        n ->
            brokerClients.clients.computeIfAbsent(
                n.id(), ignored -> MBeanClient.jndi(n.host(), brokerClients.jmxPort)));
    return Map.copyOf(brokerClients.clients);
  }

  @SuppressWarnings("resource")
  public Map<String, MBeanClient> addWorkerClients(Set<String> hostnames) {
    if (workerClients == null) return Map.of();
    hostnames.forEach(
        n ->
            workerClients.clients.computeIfAbsent(
                n, ignored -> MBeanClient.jndi(n, workerClients.jmxPort)));
    return Map.copyOf(workerClients.clients);
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

  public Map<Integer, MBeanClient> brokerClients() {
    if (brokerClients == null) return Map.of();
    return Map.copyOf(brokerClients.clients);
  }

  public Map<String, MBeanClient> workerClients() {
    if (workerClients == null) return Map.of();
    return Map.copyOf(workerClients.clients);
  }

  private static class Clients<T extends Comparable<T>> {
    private final int jmxPort;
    private final Map<T, MBeanClient> clients = new ConcurrentHashMap<>();

    Clients(int jmxPort) {
      this.jmxPort = jmxPort;
    }
  }
}
