package org.astraea.app.balancer.metrics;

import org.astraea.app.admin.ClusterInfo;
import org.astraea.app.balancer.Balancer;
import org.astraea.app.balancer.BalancerConfigs;
import org.astraea.app.balancer.BalancerUtils;
import org.astraea.app.balancer.RebalancePlanProposal;
import org.astraea.app.common.Utils;
import org.astraea.app.metrics.HasBeanObject;
import org.astraea.app.metrics.collector.Fetcher;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public interface SeekBestPlan {
    RebalancePlanProposal seek(
            double currentScore,
            ClusterInfo clusterInfo,
            Map<Fetcher, Map<Integer, Collection<HasBeanObject>>> clusterMetrics);
    static SeekBestPlan of(){
        return new SeekBestPlan() {
            @Override
            public RebalancePlanProposal seek(double currentScore, ClusterInfo clusterInfo, Map<Fetcher, Map<Integer, Collection<HasBeanObject>>> clusterMetrics , BalancerConfigs balancerConfigs) {
                var tries = balancerConfigs.rebalancePlanSearchingIteration();
                var counter = new LongAdder();
                // TODO: find a way to show the progress, without pollute the logic
                var thread = Balancer.progressWatch("Searching for Good Rebalance Plan", tries, counter::doubleValue);
                try {
                    thread.start();
                    var bestMigrationProposals =
                            planGenerator
                                    .generate(clusterInfo)
                                    .parallel()
                                    .limit(tries)
                                    .peek(ignore -> counter.increment())
                                    .map(
                                            plan -> {
                                                if (plan.rebalancePlan().isPresent()) {
                                                    var allocation = plan.rebalancePlan().get();
                                                    var mockedCluster =
                                                            BalancerUtils.mockClusterInfoAllocation(clusterInfo, allocation);
                                                    var score = evaluateCost(mockedCluster, clusterMetrics);
                                                    return Map.entry(score, plan);
                                                } else {
                                                    return Map.entry(1.0, plan);
                                                }
                                            })
                                    .filter(x -> x.getKey() < currentScore)
                                    .filter(x -> x.getValue().rebalancePlan().isPresent())
                                    .sorted(Map.Entry.comparingByKey())
                                    .limit(300)
                                    .collect(Collectors.toUnmodifiableList());

                    // Find the plan with smallest move cost
                    var bestMigrationProposal =
                            bestMigrationProposals.stream()
                                    .min(
                                            Comparator.comparing(
                                                    entry -> {
                                                        var proposal = entry.getValue();
                                                        var allocation = proposal.rebalancePlan().orElseThrow();
                                                        var mockedCluster =
                                                                BalancerUtils.mockClusterInfoAllocation(clusterInfo, allocation);
                                                        return evaluateMoveCost(mockedCluster, clusterMetrics);
                                                    }));

                    // find the target with the highest score, return it
                    return bestMigrationProposal
                            .map(Map.Entry::getValue)
                            .orElseThrow(() -> new NoSuchElementException("No Better Plan Found"));
                } finally {
                    thread.interrupt();
                    Utils.packException(() -> thread.join());
                }
            }
        }
    }
}
