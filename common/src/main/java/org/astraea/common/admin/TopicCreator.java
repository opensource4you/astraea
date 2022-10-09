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
import org.astraea.common.Utils;

public interface TopicCreator {
  // ---------------------------------[keys]---------------------------------//
  String SEGMENT_BYTES_CONFIG = "segment.bytes";
  String SEGMENT_MS_CONFIG = "segment.ms";
  String SEGMENT_JITTER_MS_CONFIG = "segment.jitter.ms";
  String SEGMENT_INDEX_BYTES_CONFIG = "segment.index.bytes";
  String FLUSH_MESSAGES_INTERVAL_CONFIG = "flush.messages";
  String FLUSH_MS_CONFIG = "flush.ms";
  String RETENTION_BYTES_CONFIG = "retention.bytes";
  String RETENTION_MS_CONFIG = "retention.ms";
  String REMOTE_LOG_STORAGE_ENABLE_CONFIG = "remote.storage.enable";
  String LOCAL_LOG_RETENTION_MS_CONFIG = "local.retention.ms";
  String LOCAL_LOG_RETENTION_BYTES_CONFIG = "local.retention.bytes";
  String MAX_MESSAGE_BYTES_CONFIG = "max.message.bytes";
  String INDEX_INTERVAL_BYTES_CONFIG = "index.interval.bytes";
  String FILE_DELETE_DELAY_MS_CONFIG = "file.delete.delay.ms";
  String DELETE_RETENTION_MS_CONFIG = "delete.retention.ms";
  String MIN_COMPACTION_LAG_MS_CONFIG = "min.compaction.lag.ms";
  String MAX_COMPACTION_LAG_MS_CONFIG = "max.compaction.lag.ms";
  String MIN_CLEANABLE_DIRTY_RATIO_CONFIG = "min.cleanable.dirty.ratio";
  String CLEANUP_POLICY_CONFIG = "cleanup.policy";
  String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = "unclean.leader.election.enable";
  String MIN_IN_SYNC_REPLICAS_CONFIG = "min.insync.replicas";
  String COMPRESSION_TYPE_CONFIG = "compression.type";
  String PREALLOCATE_CONFIG = "preallocate";
  String MESSAGE_TIMESTAMP_TYPE_CONFIG = "message.timestamp.type";
  String MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG = "message.timestamp.difference.max.ms";
  String MESSAGE_DOWNCONVERSION_ENABLE_CONFIG = "message.downconversion.enable";
  // ---------------------------------[values]---------------------------------//
  String CLEANUP_POLICY_COMPACT = "compact";
  String CLEANUP_POLICY_DELETE = "delete";
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
  @Deprecated
  default void create() {
    Utils.packException(() -> run().toCompletableFuture().get());
  }

  CompletionStage<Boolean> run();
}
