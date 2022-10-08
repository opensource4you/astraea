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
package org.astraea.common.producer;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/** An interface for sending records. */
public interface Producer<Key, Value> extends AutoCloseable {
  // ---------------------------------[keys]---------------------------------//
  String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  String CLIENT_DNS_LOOKUP_CONFIG = "client.dns.lookup";
  String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
  String METADATA_MAX_IDLE_CONFIG = "metadata.max.idle.ms";
  String BATCH_SIZE_CONFIG = "batch.size";
  String ACKS_CONFIG = "acks";
  String LINGER_MS_CONFIG = "linger.ms";
  String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
  String DELIVERY_TIMEOUT_MS_CONFIG = "delivery.timeout.ms";
  String CLIENT_ID_CONFIG = "client.id";
  String SEND_BUFFER_CONFIG = "send.buffer.bytes";
  String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
  String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
  String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
  String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
  String MAX_BLOCK_MS_CONFIG = "max.block.ms";
  String BUFFER_MEMORY_CONFIG = "buffer.memory";
  String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  String COMPRESSION_TYPE_CONFIG = "compression.type";
  String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
  String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
  String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";
  String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
  String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
  String RETRIES_CONFIG = "retries";
  String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
  String KEY_SERIALIZER_CLASS_DOC =
      "Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";
  String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
  String VALUE_SERIALIZER_CLASS_DOC =
      "Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";
  String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG = "socket.connection.setup.timeout.ms";
  String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG = "socket.connection.setup.timeout.max.ms";
  String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
  String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
  String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
  String TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms";
  String TRANSACTIONAL_ID_CONFIG = "transactional.id";
  String SECURITY_PROVIDERS_CONFIG = "security.providers";

  // ---------------------------------[values]---------------------------------//
  String COMPRESSION_TYPE_NONE = "none";
  String COMPRESSION_TYPE_GZIP = "gzip";
  String COMPRESSION_TYPE_SNAPPY = "snappy";
  String COMPRESSION_TYPE_LZ4 = "lz4";
  String COMPRESSION_TYPE_ZSTD = "zstd";

  String clientId();

  Sender<Key, Value> sender();

  /**
   * send the multiple records. Noted that the normal producer will send the record one by one. By
   * contrast, transactional producer will send all records in single transaction.
   *
   * @param senders pre-defined records
   * @return callback of all completed records
   */
  Collection<CompletionStage<Metadata>> send(Collection<Sender<Key, Value>> senders);

  /** this method is blocked until all data in buffer are sent. */
  void flush();

  void close();

  /** @return true if the producer supports transactional. */
  default boolean transactional() {
    return transactionId().isPresent();
  }

  /** @return the transaction id or empty if the producer does not support transaction. */
  Optional<String> transactionId();

  static Builder<byte[], byte[]> builder() {
    return new Builder<>();
  }

  static Producer<byte[], byte[]> of(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers).build();
  }
}
