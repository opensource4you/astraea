package org.astraea.partitioner.smoothPartitioner;

public interface DependencyClient {
  public void beginDependency();

  public void finishDependency();
}
