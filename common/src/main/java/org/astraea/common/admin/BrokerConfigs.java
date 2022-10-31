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

import java.util.Set;
import java.util.TreeSet;
import org.astraea.common.Utils;

public final class BrokerConfigs {
  // ---------------------------------[keys]---------------------------------//
  public static final String ADVERTISED_LISTENERS_CONFIG = "advertised.listeners";
  public static final String LISTENERS_CONFIG = "listeners";
  public static final String LISTENER_SECURITY_PROTOCOL_MAP_CONFIG =
      "listener.security.protocol.map";
  public static final String MAX_CONNECTIONS_CONFIG = "max.connections";
  public static final String MAX_CONNECTION_CREATION_RATE_CONFIG = "max.connection.creation.rate";
  public static final String NUM_NETWORK_THREADS_CONFIG = "num.network.threads";
  public static final String NUM_IO_THREADS_CONFIG = "num.io.threads";
  public static final String NUM_REPLICA_FETCHERS_CONFIG = "num.replica.fetchers";
  public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG =
      "num.recovery.threads.per.data.dir";
  public static final String BACKGROUND_THREADS_CONFIG = "background.threads";
  public static final String LOG_CLEANER_THREADS_CONFIG = "log.cleaner.threads";
  public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE_CONFIG =
      "log.cleaner.dedupe.buffer.size";
  public static final String LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_CONFIG =
      "log.cleaner.io.buffer.load.factor";
  public static final String LOG_CLEANER_IO_BUFFER_SIZE_CONFIG = "log.cleaner.io.buffer.size";
  public static final String MESSAGE_MAX_BYTES_CONFIG = "message.max.bytes";
  public static final String LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_CONFIG =
      "log.cleaner.io.max.bytes.per.second";
  public static final String LOG_CLEANER_BACKOFF_MS_CONFIG = "log.cleaner.backoff.ms";
  public static final String PRODUCER_ID_EXPIRATION_MS_CONFIG = "producer.id.expiration.ms";
  public static final String MAX_CONNECTIONS_PER_IP_CONFIG = "max.connections.per.ip";
  public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG =
      "max.connections.per.ip.overrides";
  public static final String LOG_SEGMENT_BYTES_CONFIG = "log.segment.bytes";
  public static final String LOG_ROLL_TIME_MILLIS_CONFIG = "log.roll.ms";
  public static final String LOG_ROLL_TIME_JITTER_MILLIS_CONFIG = "log.roll.jitter.ms";
  public static final String LOG_INDEX_SIZE_MAX_BYTES_CONFIG = "log.index.size.max.bytes";
  public static final String LOG_FLUSH_INTERVAL_MESSAGES_CONFIG = "log.flush.interval.messages";
  public static final String LOG_FLUSH_INTERVAL_MS_CONFIG = "log.flush.interval.ms";
  public static final String LOG_RETENTION_BYTES_CONFIG = "log.retention.bytes";
  public static final String LOG_RETENTION_TIME_MILLIS_CONFIG = "log.retention.ms";
  public static final String LOG_INDEX_INTERVAL_BYTES_CONFIG = "log.index.interval.bytes";
  public static final String LOG_CLEANER_DELETE_RETENTION_MS_CONFIG =
      "log.cleaner.delete.retention.ms";
  public static final String LOG_CLEANER_MIN_COMPACTION_LAG_MS_CONFIG =
      "log.cleaner.min.compaction.lag.ms";
  public static final String LOG_CLEANER_MAX_COMPACTION_LAG_MS_CONFIG =
      "log.cleaner.max.compaction.lag.ms";
  public static final String LOG_DELETE_DELAY_MS_CONFIG = "log.segment.delete.delay.ms";
  public static final String LOG_CLEANER_MIN_CLEAN_RATIO_CONFIG = "log.cleaner.min.cleanable.ratio";
  public static final String LOG_CLEANUP_POLICY_CONFIG = "log.cleanup.policy";
  public static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG =
      "unclean.leader.election.enable";
  public static final String MIN_IN_SYNC_REPLICAS_CONFIG = "min.insync.replicas";
  public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
  public static final String LOG_PRE_ALLOCATE_CONFIG = "log.preallocate";
  public static final String LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG = "log.message.timestamp.type";
  public static final String LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG =
      "log.message.timestamp.difference.max.ms";
  public static final String LOG_MESSAGE_DOWN_CONVERSION_ENABLE_CONFIG =
      "log.message.downconversion.enable";
  public static final String LEADER_REPLICATION_THROTTLED_RATE_CONFIG =
      "leader.replication.throttled.rate";
  public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG =
      "follower.replication.throttled.rate";
  public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG =
      "replica.alter.log.dirs.io.max.bytes.per.second";
  public static final Set<String> DYNAMICAL_CONFIGS =
      new TreeSet<>(Utils.constants(BrokerConfigs.class, name -> name.endsWith("CONFIG")));
  // ---------------------------------[values]---------------------------------//

  private BrokerConfigs() {}
}
