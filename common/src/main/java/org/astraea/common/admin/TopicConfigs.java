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

import java.util.List;
import org.astraea.common.Utils;

public final class TopicConfigs {

  // ---------------------------------[keys]---------------------------------//
  public static final String SEGMENT_BYTES_CONFIG = "segment.bytes";
  public static final String SEGMENT_MS_CONFIG = "segment.ms";
  public static final String SEGMENT_JITTER_MS_CONFIG = "segment.jitter.ms";
  public static final String SEGMENT_INDEX_BYTES_CONFIG = "segment.index.bytes";
  public static final String FLUSH_MESSAGES_INTERVAL_CONFIG = "flush.messages";
  public static final String FLUSH_MS_CONFIG = "flush.ms";
  public static final String RETENTION_BYTES_CONFIG = "retention.bytes";
  public static final String RETENTION_MS_CONFIG = "retention.ms";
  public static final String REMOTE_LOG_STORAGE_ENABLE_CONFIG = "remote.storage.enable";
  public static final String LOCAL_LOG_RETENTION_MS_CONFIG = "local.retention.ms";
  public static final String LOCAL_LOG_RETENTION_BYTES_CONFIG = "local.retention.bytes";
  public static final String MAX_MESSAGE_BYTES_CONFIG = "max.message.bytes";
  public static final String INDEX_INTERVAL_BYTES_CONFIG = "index.interval.bytes";
  public static final String FILE_DELETE_DELAY_MS_CONFIG = "file.delete.delay.ms";
  public static final String DELETE_RETENTION_MS_CONFIG = "delete.retention.ms";
  public static final String MIN_COMPACTION_LAG_MS_CONFIG = "min.compaction.lag.ms";
  public static final String MAX_COMPACTION_LAG_MS_CONFIG = "max.compaction.lag.ms";
  public static final String MIN_CLEANABLE_DIRTY_RATIO_CONFIG = "min.cleanable.dirty.ratio";
  public static final String CLEANUP_POLICY_CONFIG = "cleanup.policy";
  public static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG =
      "unclean.leader.election.enable";
  public static final String MIN_IN_SYNC_REPLICAS_CONFIG = "min.insync.replicas";
  public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
  public static final String PREALLOCATE_CONFIG = "preallocate";
  public static final String MESSAGE_TIMESTAMP_TYPE_CONFIG = "message.timestamp.type";
  public static final String MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG =
      "message.timestamp.difference.max.ms";
  public static final String MESSAGE_DOWNCONVERSION_ENABLE_CONFIG = "message.downconversion.enable";

  public static final String LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG =
      "leader.replication.throttled.replicas";
  public static final String FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG =
      "follower.replication.throttled.replicas";

  public static final List<String> ALL_CONFIGS =
      Utils.constants(TopicConfigs.class, name -> name.endsWith("CONFIG"));

  // ---------------------------------[values]---------------------------------//
  public static final String CLEANUP_POLICY_COMPACT = "compact";
  public static final String CLEANUP_POLICY_DELETE = "delete";

  private TopicConfigs() {}
}
