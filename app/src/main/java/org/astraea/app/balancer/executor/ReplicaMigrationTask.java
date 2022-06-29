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

import java.time.Duration;
import org.astraea.app.admin.TopicPartitionReplica;

public class ReplicaMigrationTask implements CanAwait {

  private final RebalanceAdmin admin;
  private final TopicPartitionReplica log;

  public ReplicaMigrationTask(RebalanceAdmin admin, TopicPartitionReplica log) {
    this.admin = admin;
    this.log = log;
  }

  @Override
  public boolean await(Duration timeout) {
    try {
      return admin.waitLogSynced(log, timeout);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
