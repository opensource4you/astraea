package org.astraea.yunikorn.config;

import java.util.List;

public class PartitionConfig {
  private String name;
  private List<QueueConfig> queues;

  private List<PlacementRule> placementrules;

  private List<Limit> limits;

  private PartitionPreemptionConfig preemption;

  private NodeSortingPolicy nodesortpolicy;

  private String statedumpfilepath;

  public void setName(String name) {
    this.name = name;
  }

  public void setPlacementrules(List<PlacementRule> placementrules) {
    this.placementrules = placementrules;
  }

  public void setStatedumpfilepath(String statedumpfilepath) {
    this.statedumpfilepath = statedumpfilepath;
  }

  public void setNodesortpolicy(NodeSortingPolicy nodesortpolicy) {
    this.nodesortpolicy = nodesortpolicy;
  }

  public void setQueues(List<QueueConfig> queues) {
    this.queues = queues;
  }

  public void setLimits(List<Limit> limits) {
    this.limits = limits;
  }

  public void setPreemption(PartitionPreemptionConfig preemption) {
    this.preemption = preemption;
  }

  public List<PlacementRule> getPlacementrules() {
    return placementrules;
  }

  public String getStatedumpfilepath() {
    return statedumpfilepath;
  }

  public String getName() {
    return name;
  }

  public NodeSortingPolicy getNodesortpolicy() {
    return nodesortpolicy;
  }

  public List<Limit> getLimits() {
    return limits;
  }

  public List<QueueConfig> getQueues() {
    return queues;
  }

  public PartitionPreemptionConfig getPreemption() {
    return preemption;
  }
}
