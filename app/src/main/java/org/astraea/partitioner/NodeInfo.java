package org.astraea.partitioner;

public interface NodeInfo {

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
    };
  }

  /** @return The host name for this node */
  String host();

  /** @return The id for this node */
  int id();

  /** @return The client port for this node */
  int port();
}
