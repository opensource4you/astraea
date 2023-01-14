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

import java.net.InetSocketAddress;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

public interface ZookeeperCluster extends AutoCloseable {

  static ZookeeperCluster of() {
    final NIOServerCnxnFactory factory;
    var snapshotDir = Utils.createTempDirectory("local_zk_snapshot");
    var logDir = Utils.createTempDirectory("local_zk_log");

    try {
      factory = new NIOServerCnxnFactory();
      factory.configure(new InetSocketAddress("0.0.0.0", 0), 1024);
      factory.startup(new ZooKeeperServer(snapshotDir, logDir, 500));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return new ZookeeperCluster() {
      @Override
      public void close() {
        factory.shutdown();
        Utils.delete(snapshotDir);
        Utils.delete(logDir);
      }

      @Override
      public String connectionProps() {
        return Utils.hostname() + ":" + factory.getLocalPort();
      }
    };
  }

  /**
   * @return zookeeper information. the form is "host_a:port_a,host_b:port_b"
   */
  String connectionProps();

  @Override
  void close();
}
