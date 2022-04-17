package org.astraea.balancer.alpha.executor;

import java.util.Set;
import org.astraea.balancer.alpha.BalancerUtils;
import org.astraea.balancer.alpha.ClusterLogAllocation;
import org.astraea.balancer.alpha.RebalancePlanProposal;
import org.astraea.topic.ReplicaSyncingMonitor;
import org.astraea.topic.TopicAdmin;

/** Execute every possible migration immediately. */
public class StraightPlanExecutor implements RebalancePlanExecutor {
  private final String bootstrapServer;
  private final TopicAdmin topicAdmin;

  public StraightPlanExecutor(String bootstrapServer, TopicAdmin topicAdmin) {
    this.bootstrapServer = bootstrapServer;
    this.topicAdmin = topicAdmin;
  }

  @Override
  public void run(RebalancePlanProposal proposal) {
    if (proposal.rebalancePlan().isEmpty()) return;
    final ClusterLogAllocation clusterNow =
        BalancerUtils.currentAllocation(topicAdmin, BalancerUtils.clusterSnapShot(topicAdmin));
    final ClusterLogAllocation clusterLogAllocation = proposal.rebalancePlan().get();

    clusterLogAllocation
        .allocation()
        .forEach(
            (topic, partitionReplica) -> {
              partitionReplica.forEach(
                  (partition, replicaList) -> {
                    if (clusterNow.allocation().get(topic).get(partition).equals(replicaList))
                      return;
                    System.out.printf("Move %s-%d to %s%n", topic, partition, replicaList);
                    topicAdmin
                        .migrator()
                        .partition(topic, partition)
                        .moveTo(Set.copyOf(replicaList));
                  });
            });

    // wait until everything is ok
    System.out.println("Launch Replica Syncing Monitor");
    ReplicaSyncingMonitor.main(new String[] {"--bootstrap.servers", bootstrapServer});
  }
}
