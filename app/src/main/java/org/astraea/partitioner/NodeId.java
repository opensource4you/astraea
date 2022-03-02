package org.astraea.partitioner;

/** We use this interface to replace the integer for more readable code. */
@FunctionalInterface
public interface NodeId extends Comparable<NodeInfo> {

  static NodeId of(int id) {
    return () -> id;
  }

  /** @return The id for this node */
  int id();

  @Override
  default int compareTo(NodeInfo o) {
    return Integer.compare(id(), o.id());
  }
}
