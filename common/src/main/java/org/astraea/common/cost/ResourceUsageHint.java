/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.astraea.common.cost;

import java.util.function.Predicate;
import org.astraea.common.admin.Replica;

/**
 * ResourceUsageHint is an interface that is used to determine the resource usage of a cluster and a
 * replica for a specific balancing goal. The implementation of this class will be capable of
 * providing heuristics for the balancing goal, for example:
 *
 * <ol>
 *   <li>the concrete resource usage of a given replica in terms of the current balancing problem.
 *   <li>the cluster resource usage of a given replica in terms of the current balancing problem.
 *   <li>the importance of a replica in terms of the current balancing problem.
 *   <li>the idealness of a given cluster resource usage in terms of a specific balancing goal
 *   <li>the validity of a cluster resource usage in terms of a specific balancing goal(probably a
 *       constraint)
 * </ol>
 *
 * This valuable information can be used to guide the solution search algorithm. Provide the
 * capability to discover solution spaces with higher potential to have good solutions. Or branch
 * out infeasible solutions earlier. The implementation of all these functions requires following
 * some important rules. Violating these rules might cause the algorithm to function poorly, or
 * unable to apply some optimization to speed up the search process.
 */
public interface ResourceUsageHint {

  /**
   * @return a descriptive string to describe the balancing goal or constraint of this {@link
   *     ResourceUsageHint}.
   */
  String description();

  /**
   * Determine the resource usage of a replica. Unlike {@link
   * ResourceUsageHint#evaluateClusterResourceUsage(Replica)}, the resource usage is unrelated to
   * the broker it is located at. The resource usage is somewhat fundamental to the replica itself.
   * Not incurred during the placement decision been made.
   *
   * <p>The return value is related to {@link ResourceUsageHint#importance(ResourceUsage)}. A
   * replica with more resource consumption, its location within the cluster has more impact than
   * the one with less resource consumption
   *
   * @return the resource usage of a replica.
   */
  ResourceUsage evaluateReplicaResourceUsage(Replica target);

  /**
   * Determine the resource usage incurred to a cluster when the replica is placed on its broker.
   *
   * <p>The implementation should be stateless. That is given the sample replica input. It should
   * always return the exactly same ResourceUsage content.
   *
   * @return the cluster resource usage incurred by this replica.
   */
  ResourceUsage evaluateClusterResourceUsage(Replica target);

  /**
   * Determine the importance of the resource usage incurred by a replica. The return value must be
   * within the range[0, 1]. Where 0 represents the replica will incur the highest resource
   * consumption for a certain resource among other replicas. And 1 represents the replica will
   * incur the lowest resource consumption for a certain resource among other replicas.
   */
  double importance(ResourceUsage replicaResourceUsage);

  /**
   * The idealness of a cluster resource usage state. The return value must be within the range[0,
   * 1]. Where 0 represents the most ideal usage of a certain resource(compared to the most ideal
   * final cluster). And 1 represents the most undesired usage of a certain resource(compared to the
   * most ideal final cluster).
   *
   * <p>The most ideal final cluster is a cluster that might only exist in imagination. It has all
   * the replicas placed to brokers. And the resource usage after all the placements is considered
   * in the most ideal state. <b>The idealness should be compared to the final cluster.</b> For
   * example, if we have 100 replicas, and we want to distribute it to 5 brokers evenly. In this
   * case, the most ideal final cluster we can imagine is the one that each broker has 20 replicas.
   * A correct implementation can be measuring the distance between the given cluster resource
   * usage([a,b,c,d,e]) to the most ideal usage([20,20,20,20,20]). And an inappropriate
   * implementation might be not compared to the ideal cluster. But instead, calculate the average
   * between the given resource usages avg=(a+b+c+d+e)/5. And measuring the distance between
   * [a,b,c,d,e] to [avg,avg,avg,avg,avg]. The inappropriate implementation thinks the given cluster
   * resource usage is the one where all the resources are set. But this {@link ResourceUsageHint}
   * framework doesn't enforce this.
   */
  double idealness(ResourceUsage clusterResourceUsage);

  /**
   * A {@link Predicate} to determine the validity of a cluster resource usage. Noted that this
   * Predicate can only describe the exceeded usage of resources. It can describe constraints like:
   * each broker can only have at most 30 leaders. But it can't describe logic like: each broker
   * must have at least 30 leaders. With the requirement we can conclude a general law for the
   * return predicate, given an empty {@link ResourceUsage}, it should always return true.
   *
   * <p>It is recommended to use {@link ResourceUsageHint#idealness(ResourceUsage)} over this
   * method, if the balancing goal can be described in that way.
   *
   * @return a {@link Predicate} that can use to determine if the resource usage of a cluster is
   *     valid.
   */
  default Predicate<ResourceUsage> usageValidityPredicate() {
    return (usage) -> true;
  }
}
