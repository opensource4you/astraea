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
 * ResourceUsageHint is an interface used to determine the resource usage of a cluster and a replica
 * for a specific balancing goal. Implementations of this class provide heuristics for the balancing
 * goal, such as:
 *
 * <ol>
 *   <li>The concrete resource usage of a given replica in terms of the current balancing problem.
 *   <li>The cluster resource usage of a given replica in terms of the current balancing problem.
 *   <li>The importance of a replica in terms of the current balancing problem.
 *   <li>The idealness of a given cluster resource usage in terms of a specific balancing goal
 *   <li>The validity of a cluster resource usage in terms of a specific balancing goal(possibly a
 *       constraint)
 * </ol>
 *
 * This valuable information can guide the solution search algorithm. Enabling it to discover
 * solution spaces with higher potential for good solutions or branch out infeasible solutions
 * earlier. Implementations of all these functions require following some important rules. Violating
 * these rules might cause the algorithm to function poorly or unable to apply some optimizations to
 * speed up the search process.
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
   *
   * <p>The return value should be used in conjunction with {@link
   * ResourceUsageHint#importance(ResourceUsage)}. A replica with more resource consumption, its
   * location within the cluster will be more impactful than the one with less resource consumption.
   * Which generally results in a higher importance score.
   *
   * @return the resource usage of a replica.
   */
  ResourceUsage evaluateReplicaResourceUsage(Replica target);

  /**
   * Determine the resource usage incurred to a cluster when the replica is placed on its broker.
   *
   * <p>The implementation should be stateless, meaning that given the sample input, it should
   * always return the same resource usage.
   *
   * @return the cluster resource usage incurred by this replica.
   */
  ResourceUsage evaluateClusterResourceUsage(Replica target);

  /**
   * Determine the importance of the resource usage that will be incurred a replica. The return
   * value must be within the range [0, 1]. Where 0 represents the replica associated with the given
   * resource usage will incur the highest resource consumption for a certain resource among other
   * replicas. And 1 represents the replica will incur the lowest resource consumption for a certain
   * resource among other replicas.
   */
  double importance(ResourceUsage replicaResourceUsage);

  /**
   * Compute the idealness score of the given cluster resource usage based on the balancing goal of
   * this {@link ResourceUsageHint}. The return value must be within the range [0, 1]. Where 0
   * represents the ideal usage of a certain resource(compared to the ideal complete cluster
   * resource usage). And given two idealness scores of two cluster resource usage, <code>a</code>
   * and <code>b</code>. If <code>a < b</code>. This means the cluster resource allocation of <code>
   * a
   * </code> is considered more ideal than the one with <code>b</code>, in terms of the specific
   * balancing goal.
   *
   * <p>The term <b>complete cluster resource usage</b> refers to a cluster resource usage where all
   * the replicas resource usage is covered. And the definition of a <b>ideal complete cluster
   * resource usage</b> is where the complete cluster resource usage is considered ideal in terms of
   * the balancing goal of this {@link ResourceUsageHint}.
   *
   * <p>For example, Suppose the {@link ResourceUsageHint} is about balancing the number of replicas
   * between brokers. Then given 100 replicas and 5 brokers. The ideal resource placement will be
   * {20, 20, 20, 20, 20}. In which the replicas are spread evenly. An example of an appropriate
   * implementation can calculate the distance between the current placement {a,b,c,d,e} to
   * {20,20,20,20,20}. If all value equals to 20, The idealness will be 0, which means the current
   * resource usage is considered ideal. On the other hand, an <b>inappropriate</b> implementation
   * might be measuring the distance between {a,b,c,d,e} to {avg,avg,avg,avg,avg} where avg is
   * (a+b+c+d+e)/5. The problem with this approach is, the sum of the input resource usage might not
   * be 100. The {@link ResourceUsageHint} framework doesn't require input resource usage be
   * <b>complete</b>(to contain all the replicas).
   */
  double idealness(ResourceUsage clusterResourceUsage);

  /**
   * A {@link Predicate} to determine the validity of a cluster resource usage. Noted that this
   * Predicate can only describe on exceeded usage of resources. Such as limiting the number of
   * leader replicas a broker can have. It cannot describe logic such as requiring a minimum number
   * of leaders per broker must have. With the requirement we can conclude a general law for the
   * return predicate, given an empty {@link ResourceUsage}, it should always return true.
   *
   * <p>It is recommended to use {@link ResourceUsageHint#idealness(ResourceUsage)} over this method
   * if possible.
   *
   * @return a {@link Predicate} that can use to determine if the resource usage of a cluster is
   *     valid.
   */
  default Predicate<ResourceUsage> usageValidityPredicate() {
    return (usage) -> true;
  }
}
