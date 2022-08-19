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
package org.astraea.app.admin;

import java.util.Objects;

public interface NodeInfo extends Comparable<NodeInfo> {

  static NodeInfo of(org.apache.kafka.common.Node node) {
    return of(node.id(), node.host(), node.port());
  }

  /**
   * Represent a location unknown Kafka node. The node used to be in the cluster but probably become
   * offline and unreachable for some reason.
   */
  static NodeInfo ofOfflineNode(int id) {
    return of(id, "", -1, true);
  }

  static NodeInfo of(int id, String host, int port) {
    return of(id, host, port, false);
  }

  static NodeInfo of(int id, String host, int port, boolean isOffline) {
    return new NodeInfo() {
      // NodeInfo is used to be key of Map commonly, so creating hash can reduce the memory pressure
      private final int hashCode = Objects.hash(id, host, port);

      @Override
      public String host() {
        return host;
      }

      @Override
      public int id() {
        return id;
      }

      @Override
      public boolean isOffline() {
        return isOffline;
      }

      @Override
      public int port() {
        return port;
      }

      @Override
      public int hashCode() {
        return hashCode;
      }

      @Override
      public boolean equals(Object other) {
        if (other instanceof NodeInfo) return compareTo((NodeInfo) other) == 0;
        return false;
      }

      @Override
      public int compareTo(NodeInfo other) {
        int r = Integer.compare(id(), other.id());
        if (r != 0) return r;
        r = host().compareTo(other.host());
        if (r != 0) return r;
        return Integer.compare(port(), other.port());
      }
    };
  }

  /** @return The host name for this node */
  String host();

  /** @return The client (kafka data, jmx, etc.) port for this node */
  int port();

  /** @return id of broker node. it must be unique. */
  int id();

  /** @return true if the node is considered offline */
  boolean isOffline();

  /** @return true if the node is considered online */
  default boolean isOnline() {
    return !isOffline();
  }
}
