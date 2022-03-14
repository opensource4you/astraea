package org.astraea.yunikorn.config;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class PartitionConfig {
  private String name;
  private List<QueueConfig> queues;

  private List<PlacementRule> placementrules;

  private List<Limit> limits;

  private PartitionPreemptionConfig preemption;

  private NodeSortingPolicy nodesortpolicy;

  private String statedumpfilepath;
}
