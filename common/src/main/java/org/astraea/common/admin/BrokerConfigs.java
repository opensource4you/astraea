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
  // ====================================================================
  // Log (Retention/Cleaning/Storage) Configurations
  // ====================================================================

  /** General Retention/Policy */
  public static final String LOG_CLEANUP_POLICY_CONFIG = "log.cleanup.policy"; // delete, compact

  public static final String LOG_RETENTION_MS_CONFIG = "log.retention.ms";
  public static final String LOG_RETENTION_BYTES_CONFIG = "log.retention.bytes";
  public static final String LOG_LOCAL_RETENTION_MS_CONFIG = "log.local.retention.ms";
  public static final String LOG_LOCAL_RETENTION_BYTES_CONFIG = "log.local.retention.bytes";

  /** Segment Rolling/Size */
  public static final String LOG_SEGMENT_BYTES_CONFIG = "log.segment.bytes";

  public static final String LOG_ROLL_MS_CONFIG = "log.roll.ms";
  public static final String LOG_ROLL_JITTER_MS_CONFIG = "log.roll.jitter.ms";
  public static final String LOG_SEGMENT_DELETE_DELAY_MS_CONFIG = "log.segment.delete.delay.ms";
  public static final String LOG_PREALLOCATE_CONFIG = "log.preallocate";

  /** Log Cleaner (Compaction) */
  public static final String LOG_CLEANER_THREADS_CONFIG = "log.cleaner.threads";

  public static final String LOG_CLEANER_DELETE_RETENTION_MS_CONFIG =
      "log.cleaner.delete.retention.ms";
  public static final String LOG_CLEANER_MIN_CLEANABLE_RATIO_CONFIG =
      "log.cleaner.min.cleanable.ratio";
  public static final String LOG_CLEANER_IO_BUFFER_SIZE_CONFIG = "log.cleaner.io.buffer.size";
  public static final String LOG_CLEANER_IO_BUFFER_LOAD_FACTOR_CONFIG =
      "log.cleaner.io.buffer.load.factor";
  public static final String LOG_CLEANER_DEDUPE_BUFFER_SIZE_CONFIG =
      "log.cleaner.dedupe.buffer.size";
  public static final String LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_CONFIG =
      "log.cleaner.io.max.bytes.per.second";
  public static final String LOG_CLEANER_BACKOFF_MS_CONFIG = "log.cleaner.backoff.ms";
  public static final String LOG_CLEANER_MIN_COMPACTION_LAG_MS_CONFIG =
      "log.cleaner.min.compaction.lag.ms";
  public static final String LOG_CLEANER_MAX_COMPACTION_LAG_MS_CONFIG =
      "log.cleaner.max.compaction.lag.ms";

  /** Log Index */
  public static final String LOG_INDEX_INTERVAL_BYTES_CONFIG = "log.index.interval.bytes";

  public static final String LOG_INDEX_SIZE_MAX_BYTES_CONFIG = "log.index.size.max.bytes";

  /** Log Flush */
  public static final String LOG_FLUSH_INTERVAL_MESSAGES_CONFIG = "log.flush.interval.messages";

  public static final String LOG_FLUSH_INTERVAL_MS_CONFIG = "log.flush.interval.ms";

  /** Message Timestamp */
  public static final String LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG = "log.message.timestamp.type";

  public static final String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG =
      "log.message.timestamp.before.max.ms";
  public static final String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG =
      "log.message.timestamp.after.max.ms";

  // ====================================================================
  // Network & Threading Configurations
  // ====================================================================

  public static final String LISTENERS_CONFIG = "listeners";
  public static final String LISTENER_SECURITY_PROTOCOL_MAP_CONFIG =
      "listener.security.protocol.map";
  public static final String NUM_NETWORK_THREADS_CONFIG = "num.network.threads";
  public static final String NUM_IO_THREADS_CONFIG = "num.io.threads";
  public static final String BACKGROUND_THREADS_CONFIG = "background.threads";
  public static final String MAX_CONNECTIONS_CONFIG = "max.connections";
  public static final String MAX_CONNECTIONS_PER_IP_CONFIG = "max.connections.per.ip";
  public static final String MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG =
      "max.connections.per.ip.overrides";
  public static final String MAX_CONNECTION_CREATION_RATE_CONFIG = "max.connection.creation.rate";
  public static final String MESSAGE_MAX_BYTES_CONFIG = "message.max.bytes";

  // ====================================================================
  // SSL/TLS Security Configurations
  // ====================================================================

  public static final String SSL_CLIENT_AUTH_CONFIG = "ssl.client.auth";
  public static final String SSL_PROTOCOL_CONFIG = "ssl.protocol";
  public static final String SSL_ENABLED_PROTOCOLS_CONFIG = "ssl.enabled.protocols";
  public static final String SSL_CIPHER_SUITES_CONFIG = "ssl.cipher.suites";
  public static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG =
      "ssl.endpoint.identification.algorithm";
  public static final String SSL_PROVIDER_CONFIG = "ssl.provider";
  public static final String SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG =
      "ssl.secure.random.implementation";
  public static final String SSL_ENGINE_FACTORY_CLASS_CONFIG = "ssl.engine.factory.class";

  /** Truststore */
  public static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";

  public static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
  public static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
  public static final String SSL_TRUSTSTORE_CERTIFICATES_CONFIG = "ssl.truststore.certificates";
  public static final String SSL_TRUSTMANAGER_ALGORITHM_CONFIG = "ssl.trustmanager.algorithm";

  /** Keystore */
  public static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";

  public static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
  public static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
  public static final String SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG =
      "ssl.keystore.certificate.chain";
  public static final String SSL_KEYSTORE_KEY_CONFIG = "ssl.keystore.key";
  public static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
  public static final String SSL_KEYMANAGER_ALGORITHM_CONFIG = "ssl.keymanager.algorithm";

  // ====================================================================
  // SASL Security Configurations
  // ====================================================================

  public static final String SASL_ENABLED_MECHANISMS_CONFIG = "sasl.enabled.mechanisms";
  public static final String SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG =
      "sasl.mechanism.inter.broker.protocol";
  public static final String PRINCIPAL_BUILDER_CLASS_CONFIG = "principal.builder.class";
  public static final String SASL_JAAS_CONFIG_CONFIG = "sasl.jaas.config";

  /** Kerberos */
  public static final String SASL_KERBEROS_SERVICE_NAME_CONFIG = "sasl.kerberos.service.name";

  public static final String SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG =
      "sasl.kerberos.principal.to.local.rules";
  public static final String SASL_KERBEROS_KINIT_CMD_CONFIG = "sasl.kerberos.kinit.cmd";
  public static final String SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN_CONFIG =
      "sasl.kerberos.min.time.before.relogin";
  public static final String SASL_KERBEROS_TICKET_RENEW_JITTER_CONFIG =
      "sasl.kerberos.ticket.renew.jitter";
  public static final String SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR_CONFIG =
      "sasl.kerberos.ticket.renew.window.factor";

  /** Login Refresh */
  public static final String SASL_LOGIN_REFRESH_WINDOW_FACTOR_CONFIG =
      "sasl.login.refresh.window.factor";

  public static final String SASL_LOGIN_REFRESH_WINDOW_JITTER_CONFIG =
      "sasl.login.refresh.window.jitter";
  public static final String SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS_CONFIG =
      "sasl.login.refresh.min.period.seconds";
  public static final String SASL_LOGIN_REFRESH_BUFFER_SECONDS_CONFIG =
      "sasl.login.refresh.buffer.seconds";

  // ====================================================================
  // Replication & Controller Configurations
  // ====================================================================

  public static final String NUM_REPLICA_FETCHERS_CONFIG = "num.replica.fetchers";
  public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG =
      "num.recovery.threads.per.data.dir";
  public static final String MIN_INSYNC_REPLICAS_CONFIG = "min.insync.replicas";
  public static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG =
      "unclean.leader.election.enable";

  // ====================================================================
  // Compression Configurations
  // ====================================================================

  public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
  public static final String COMPRESSION_GZIP_LEVEL_CONFIG = "compression.gzip.level";
  public static final String COMPRESSION_LZ4_LEVEL_CONFIG = "compression.lz4.level";
  public static final String COMPRESSION_ZSTD_LEVEL_CONFIG = "compression.zstd.level";

  // ====================================================================
  // Transaction Configurations
  // ====================================================================

  public static final String TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG =
      "transaction.partition.verification.enable";
  public static final String PRODUCER_ID_EXPIRATION_MS_CONFIG = "producer.id.expiration.ms";

  // ====================================================================
  // Metrics & General Utilities
  // ====================================================================

  public static final String METRIC_REPORTERS_CONFIG = "metric.reporters";
  public static final String CONFIG_PROVIDERS_CONFIG = "config.providers";

  // ====================================================================
  // Remote Storage (Tiered Storage) Configurations
  // ====================================================================

  public static final String REMOTE_LOG_READER_THREADS_CONFIG = "remote.log.reader.threads";
  public static final String REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_CONFIG =
      "remote.log.manager.copier.thread.pool.size";
  public static final String REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_CONFIG =
      "remote.log.manager.follower.thread.pool.size";
  public static final String REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_CONFIG =
      "remote.log.manager.expiration.thread.pool.size";
  public static final String REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_CONFIG =
      "remote.log.manager.copy.max.bytes.per.second";
  public static final String REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_CONFIG =
      "remote.log.manager.fetch.max.bytes.per.second";
  public static final String REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_CONFIG =
      "remote.log.index.file.cache.total.size.bytes";
  public static final String REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_CONFIG =
      "remote.list.offsets.request.timeout.ms";
  public static final String REMOTE_FETCH_MAX_WAIT_MS_CONFIG = "remote.fetch.max.wait.ms";

  // ====================================================================
  // Coordinator Cache Configurations
  // ====================================================================

  public static final String GROUP_COORDINATOR_CACHED_BUFFER_MAX_BYTES_CONFIG =
      "group.coordinator.cached.buffer.max.bytes";
  public static final String SHARE_COORDINATOR_CACHED_BUFFER_MAX_BYTES_CONFIG =
      "share.coordinator.cached.buffer.max.bytes";

  // ====================================================================
  // Throttle Configurations
  // ====================================================================

  public static final String LEADER_REPLICATION_THROTTLED_RATE_CONFIG =
      "leader.replication.throttled.rate";
  public static final String FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG =
      "follower.replication.throttled.rate";
  public static final String REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG =
      "replica.alter.log.dirs.io.max.bytes.per.second";

  public static final Set<String> DYNAMICAL_CONFIGS =
      new TreeSet<>(
          Utils.constants(BrokerConfigs.class, name -> name.endsWith("CONFIG"), String.class));

  private BrokerConfigs() {}
}
