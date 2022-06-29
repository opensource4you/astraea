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
package org.astraea.app.balancer.executor;

import org.astraea.app.balancer.log.ClusterLogAllocation;

/** The states and variables related to current rebalance execution */
public interface RebalanceExecutionContext {

  static RebalanceExecutionContext of(
      RebalanceAdmin rebalanceAdmin, ClusterLogAllocation expectedAllocation) {
    return new RebalanceExecutionContext() {
      @Override
      public RebalanceAdmin rebalanceAdmin() {
        return rebalanceAdmin;
      }

      @Override
      public ClusterLogAllocation expectedAllocation() {
        return expectedAllocation;
      }
    };
  }

  /** The migration interface */
  RebalanceAdmin rebalanceAdmin();

  /** The expected log allocation after the rebalance execution */
  ClusterLogAllocation expectedAllocation();

  // TODO: add a method to fetch balancer related configuration
}
