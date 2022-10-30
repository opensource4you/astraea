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
package org.astraea.common.consumer;

import java.util.Set;
import org.astraea.common.Utils;

public final class ConsumerConfigs {

  // ---------------------------------[keys]---------------------------------//
  public static final String GROUP_ID_CONFIG = "group.id";
  public static final String GROUP_INSTANCE_ID_CONFIG = "group.instance.id";
  public static final String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
  public static final String MAX_POLL_INTERVAL_MS_CONFIG = "max.poll.interval.ms";
  public static final String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
  public static final String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  public static final String CLIENT_DNS_LOOKUP_CONFIG = "client.dns.lookup";
  public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
  public static final String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";
  public static final String PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy";
  public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
  public static final String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
  public static final String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
  public static final String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
  public static final String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
  public static final String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";
  public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";
  public static final String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
  public static final String CLIENT_ID_CONFIG = "client.id";
  public static final String CLIENT_RACK_CONFIG = "client.rack";
  public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
  public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
  public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
  public static final String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";
  public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
  public static final String CHECK_CRCS_CONFIG = "check.crcs";
  public static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
  public static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
  public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG =
      "socket.connection.setup.timeout.ms";
  public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG =
      "socket.connection.setup.timeout.max.ms";
  public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
  public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
  public static final String DEFAULT_API_TIMEOUT_MS_CONFIG = "default.api.timeout.ms";
  public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
  public static final String EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics";
  public static final String ISOLATION_LEVEL_CONFIG = "isolation.level";
  public static final String ALLOW_AUTO_CREATE_TOPICS_CONFIG = "allow.auto.create.topics";

  public static final Set<String> ALL_CONFIGS =
      Utils.constants(ConsumerConfigs.class, name -> name.endsWith("CONFIG"));

  // ---------------------------------[Values]---------------------------------//
  public static final String AUTO_OFFSET_RESET_LATEST = "latest";
  public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
  public static final String AUTO_OFFSET_RESET_NONE = "none";
  public static final String ISOLATION_LEVEL_UNCOMMITTED = "read_uncommitted";
  public static final String ISOLATION_LEVEL_COMMITTED = "read_committed";

  private ConsumerConfigs() {}
}
