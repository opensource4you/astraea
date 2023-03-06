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
package org.astraea.it;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.rmi.registry.LocateRegistry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

public interface Service extends AutoCloseable {
  static Builder builder() {
    return new Builder();
  }

  // ------------------[JMX]------------------//

  JMXServiceURL jmxServiceURL();

  // ------------------[Broker]------------------//

  String bootstrapServers();

  Map<Integer, Set<String>> dataFolders();

  void close(int brokerId);

  // ------------------[Worker]------------------//

  default URL workerUrl() {
    return List.copyOf(workerUrls()).get((int) (Math.random() * workerUrls().size()));
  }

  Set<URL> workerUrls();

  class Builder {
    private int numberOfBrokers = 1;

    private Map<String, String> brokerConfigs = Map.of();

    private int numberOfWorkers = 1;

    private Map<String, String> workerConfigs = Map.of();

    public Builder numberOfBrokers(int numberOfBrokers) {
      this.numberOfBrokers = numberOfBrokers;
      return this;
    }

    public Builder brokerConfigs(Map<String, String> brokerConfigs) {
      this.brokerConfigs = brokerConfigs;
      return this;
    }

    public Builder numberOfWorkers(int numberOfWorkers) {
      this.numberOfWorkers = numberOfWorkers;
      return this;
    }

    public Builder workerConfigs(Map<String, String> workerConfigs) {
      this.workerConfigs = workerConfigs;
      return this;
    }

    public Service build() {
      return new Service() {
        private JMXConnectorServer jmxConnectorServer = jmxConnectorServer(Utils.availablePort());

        private BrokerCluster brokerCluster;
        private WorkerCluster workerCluster;

        @Override
        public synchronized JMXServiceURL jmxServiceURL() {
          if (jmxConnectorServer == null)
            jmxConnectorServer = jmxConnectorServer(Utils.availablePort());
          return jmxConnectorServer.getAddress();
        }

        @Override
        public String bootstrapServers() {
          tryToCreateBrokerCluster();
          return brokerCluster.bootstrapServers();
        }

        @Override
        public Map<Integer, Set<String>> dataFolders() {
          tryToCreateBrokerCluster();
          return brokerCluster.dataFolders();
        }

        @Override
        public void close(int brokerId) {
          tryToCreateBrokerCluster();
          brokerCluster.close(brokerId);
        }

        @Override
        public Set<URL> workerUrls() {
          tryToCreateWorkerCluster();
          return workerCluster.workerUrls();
        }

        @Override
        public synchronized void close() {
          if (workerCluster != null) workerCluster.close();
          if (brokerCluster != null) brokerCluster.close();
          if (jmxConnectorServer != null) {
            try {
              jmxConnectorServer.stop();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }

        synchronized void tryToCreateBrokerCluster() {
          if (brokerCluster == null)
            brokerCluster = BrokerCluster.of(numberOfBrokers, brokerConfigs);
        }

        synchronized void tryToCreateWorkerCluster() {
          tryToCreateBrokerCluster();
          if (workerCluster == null)
            workerCluster = WorkerCluster.of(brokerCluster, numberOfWorkers, workerConfigs);
        }
      };
    }

    private static JMXConnectorServer jmxConnectorServer(int port) {
      try {
        LocateRegistry.createRegistry(port);
        var mBeanServer = ManagementFactory.getPlatformMBeanServer();
        var jmxServer =
            JMXConnectorServerFactory.newJMXConnectorServer(
                // we usually use JNDI to get metrics in production, so the embedded server should
                // use
                // JNDI too.
                new JMXServiceURL(
                    String.format(
                        "service:jmx:rmi://%s:%s/jndi/rmi://%s:%s/jmxrmi",
                        address(), port, address(), port)),
                null,
                mBeanServer);
        jmxServer.start();
        return jmxServer;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private static String address() {
      try {
        var address = InetAddress.getLocalHost().getHostAddress();
        if (address.equals("0.0.0.0") || address.equals("127.0.0.1"))
          throw new RuntimeException("the address of host can't be either 0.0.0.0 or 127.0.0.1");
        return InetAddress.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  void close();
}
