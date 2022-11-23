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
package org.astraea.common.admin;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** A builder for building a fake {@link ClusterInfo}. */
public class ClusterInfoBuilder<T extends ReplicaInfo> {

  private final ClusterInfo<T> sourceCluster;
  private final List<ClusterInfoAlteration<T>> alterations;

  private ClusterInfoBuilder(ClusterInfo<T> source) {
    this.sourceCluster = source;
    this.alterations = new ArrayList<>();
  }

  public static ClusterInfoBuilder<Replica> builder() {
    return builder(ClusterInfo.empty());
  }

  public static <T extends ReplicaInfo> ClusterInfoBuilder<T> builder(ClusterInfo<T> source) {
    return new ClusterInfoBuilder<>(source);
  }

  public ClusterInfoBuilder<T> apply(ClusterInfoAlteration<T> alteration) {
    this.alterations.add(alteration);
    return this;
  }

  public ClusterInfo<T> build() {
    final var nodes = new HashSet<>(sourceCluster.nodes());
    final var replicas = new ArrayList<>(sourceCluster.replicas());
    alterations.forEach(alteration -> alteration.apply(nodes, replicas));

    return ClusterInfo.of(nodes, replicas);
  }
}
