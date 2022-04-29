package org.astraea.cost;

import java.util.Objects;
import java.util.Optional;

public interface ReplicaInfo extends Comparable<ReplicaInfo> {
  /**
   * The node that serving this replica
   *
   * @return a {@link NodeInfo} of a broker that store this replica.
   */
  NodeInfo nodeInfo();

  /**
   * The path to the data folder which hosts this replica. Since this information is not very openly
   * available. An application might find it hard to retrieve this information(for example, the
   * producer client might need to initialize an AdminClient to access this information). To provide
   * this information or not is totally up to the caller, If anyone has problem access this info,
   * try use {@link #of(NodeInfo)} to constructor the {@link ReplicaInfo} object.
   *
   * @return a {@link Optional<String>} that indicates the data folder path which stored this
   *     replica on a specific Kafka node.
   */
  Optional<String> dataFolder();

  /**
   * Create a {@link ReplicaInfo} without the information of the data folder which stored this
   * replica.
   *
   * @param node the Kafka node which hosts this replica.
   */
  static ReplicaInfo of(NodeInfo node) {
    return of(node, null);
  }

  /**
   * Create a {@link ReplicaInfo}.
   *
   * @param node the Kafka node which hosts this replica.
   * @param dataFolder the data folder path which stored this replica.
   */
  static ReplicaInfo of(NodeInfo node, String dataFolder) {
    Objects.requireNonNull(node);
    return new ReplicaInfo() {
      @Override
      public NodeInfo nodeInfo() {
        return node;
      }

      @Override
      public Optional<String> dataFolder() {
        return Optional.ofNullable(dataFolder);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof ReplicaInfo) {
          ReplicaInfo that = (ReplicaInfo) obj;
          return this.nodeInfo().equals(that.nodeInfo())
              && this.dataFolder().equals(that.dataFolder());
        }
        return false;
      }

      @Override
      public String toString() {
        return "ReplicaInfo{ nodeInfo=" + nodeInfo() + ", dataFolder=" + dataFolder() + "}";
      }

      @Override
      public int compareTo(ReplicaInfo that) {
        Objects.requireNonNull(that);
        int compare0 = this.nodeInfo().compareTo(that.nodeInfo());
        if (compare0 != 0) return compare0;
        if (this.dataFolder().isEmpty() && that.dataFolder().isEmpty()) return 0;
        if (this.dataFolder().isEmpty()) return -1;
        if (that.dataFolder().isEmpty()) return 1;
        return this.dataFolder().get().compareTo(that.dataFolder().get());
      }
    };
  }
}
