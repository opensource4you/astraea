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

import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.config.TopicConfig;
import org.astraea.common.Utils;

public interface TopicCreator {

  // ---------------------------------[keys]---------------------------------//

  String CLEANUP_POLICY_CONFIG = TopicConfig.CLEANUP_POLICY_CONFIG;
  String MAX_COMPACTION_LAG_MS_CONFIG = TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG;
  String COMPRESSION_TYPE_CONFIG = TopicConfig.COMPRESSION_TYPE_CONFIG;

  // ---------------------------------[values]---------------------------------//

  String CLEANUP_POLICY_COMPACT = TopicConfig.CLEANUP_POLICY_COMPACT;
  String CLEANUP_POLICY_DELETE = TopicConfig.CLEANUP_POLICY_DELETE;

  // ---------------------------------[methods]---------------------------------//

  TopicCreator topic(String topic);

  TopicCreator numberOfPartitions(int numberOfPartitions);

  TopicCreator numberOfReplicas(short numberOfReplicas);

  /**
   * @param configs used to create new topic
   * @return this creator
   */
  TopicCreator configs(Map<String, String> configs);

  /** start to create topic. */
  default void create() {
    Utils.packException(() -> run().toCompletableFuture().get());
  }

  CompletionStage<Boolean> run();
}
