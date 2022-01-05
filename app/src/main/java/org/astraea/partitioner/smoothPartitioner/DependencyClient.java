package org.astraea.partitioner.smoothPartitioner;

/**
 * The interface for the {@link SmoothWeightPartitioner}
 *
 * @see SmoothWeightPartitioner Enable users to send dependent data.
 */
public interface DependencyClient {
  void finishDependency();
}
