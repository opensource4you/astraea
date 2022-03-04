package org.astraea.partitioner;

import java.util.Objects;

public interface NodeInfo extends Comparable<NodeInfo>, NodeId {

  static NodeInfo of(org.apache.kafka.common.Node node) {
    return of(node.id(), node.host(), node.port());
  }

  static NodeInfo of(int id, String host, int port) {
    return new NodeInfo() {
      @Override
      public String host() {
        return host;
      }

      @Override
      public int id() {
        return id;
      }

      @Override
      public int port() {
        return port;
      }

      @Override
      public int hashCode() {
        return Objects.hash(id, host, port);
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
}
