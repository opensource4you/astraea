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

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import org.astraea.common.admin.TopicPartition;

/** An interface for polling records. */
public interface Consumer<Key, Value> extends AutoCloseable {

  // ---------------------------------[keys]---------------------------------//
  String GROUP_ID_CONFIG = "group.id";
  String GROUP_INSTANCE_ID_CONFIG = "group.instance.id";
  String MAX_POLL_RECORDS_CONFIG = "max.poll.records";
  String MAX_POLL_INTERVAL_MS_CONFIG = "max.poll.interval.ms";
  String SESSION_TIMEOUT_MS_CONFIG = "session.timeout.ms";
  String HEARTBEAT_INTERVAL_MS_CONFIG = "heartbeat.interval.ms";
  String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  String CLIENT_DNS_LOOKUP_CONFIG = "client.dns.lookup";
  String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";
  String AUTO_COMMIT_INTERVAL_MS_CONFIG = "auto.commit.interval.ms";
  String PARTITION_ASSIGNMENT_STRATEGY_CONFIG = "partition.assignment.strategy";
  String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";
  String FETCH_MIN_BYTES_CONFIG = "fetch.min.bytes";
  String FETCH_MAX_BYTES_CONFIG = "fetch.max.bytes";
  String FETCH_MAX_WAIT_MS_CONFIG = "fetch.max.wait.ms";
  String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
  String MAX_PARTITION_FETCH_BYTES_CONFIG = "max.partition.fetch.bytes";
  String SEND_BUFFER_CONFIG = "send.buffer.bytes";
  String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
  String CLIENT_ID_CONFIG = "client.id";
  String CLIENT_RACK_CONFIG = "client.rack";
  String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
  String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
  String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
  String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
  String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";
  String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
  String CHECK_CRCS_CONFIG = "check.crcs";
  String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
  String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
  String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = "socket.connection.setup.timeout.ms";
  String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = "socket.connection.setup.timeout.max.ms";
  String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
  String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
  String DEFAULT_API_TIMEOUT_MS_CONFIG = "default.api.timeout.ms";
  String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
  String EXCLUDE_INTERNAL_TOPICS_CONFIG = "exclude.internal.topics";
  String ISOLATION_LEVEL_CONFIG = "isolation.level";
  String ALLOW_AUTO_CREATE_TOPICS_CONFIG = "allow.auto.create.topics";

  // ---------------------------------[Values]---------------------------------//
  String AUTO_OFFSET_RESET_LATEST = "latest";
  String AUTO_OFFSET_RESET_EARLIEST = "earliest";
  String AUTO_OFFSET_RESET_NONE = "none";
  String ISOLATION_LEVEL_UNCOMMITTED = "read_uncommitted";
  String ISOLATION_LEVEL_COMMITTED = "read_committed";

  default Collection<Record<Key, Value>> poll(Duration timeout) {
    return poll(1, timeout);
  }

  /**
   * try to poll data until there are enough records to return or the timeout is reached.
   *
   * @param recordCount max number of returned records.
   * @param timeout max time to wait data
   * @return records
   */
  Collection<Record<Key, Value>> poll(int recordCount, Duration timeout);

  /**
   * Wakeup the consumer. This method is thread-safe and is useful in particular to abort a long
   * poll. The thread which is blocking in an operation will throw {@link
   * org.apache.kafka.common.errors.WakeupException}. If no thread is blocking in a method which can
   * throw {@link org.apache.kafka.common.errors.WakeupException}, the next call to such a method
   * will raise it instead.
   */
  void wakeup();

  @Override
  void close();

  /** resubscribe partitions or rejoin the consumer group. */
  void resubscribe();

  /** unsubscribe all partitions. */
  void unsubscribe();

  /** @return current partitions assigned to this consumer */
  Set<TopicPartition> assignments();

  /** @return client id of this consumer */
  String clientId();

  /**
   * Create a consumer builder by setting specific topics
   *
   * @param topics set of topic names
   * @return consumer builder for topics
   */
  static TopicsBuilder<byte[], byte[]> forTopics(Set<String> topics) {
    return new TopicsBuilder<>(topics);
  }

  /**
   * Create a consumer builder by setting specific topic partitions
   *
   * @param partitions set of TopicPartition
   * @return consumer builder for partitions
   */
  static PartitionsBuilder<byte[], byte[]> forPartitions(Set<TopicPartition> partitions) {
    return new PartitionsBuilder<>(partitions);
  }
}
