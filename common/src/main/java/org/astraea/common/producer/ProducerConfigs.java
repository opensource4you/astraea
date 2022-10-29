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

import java.util.List;
import org.astraea.common.Utils;

public final class ProducerConfigs {
  // ---------------------------------[keys]---------------------------------//
  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
  public static final String CLIENT_DNS_LOOKUP_CONFIG = "client.dns.lookup";
  public static final String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
  public static final String METADATA_MAX_IDLE_CONFIG = "metadata.max.idle.ms";
  public static final String BATCH_SIZE_CONFIG = "batch.size";
  public static final String ACKS_CONFIG = "acks";
  public static final String LINGER_MS_CONFIG = "linger.ms";
  public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
  public static final String DELIVERY_TIMEOUT_MS_CONFIG = "delivery.timeout.ms";
  public static final String CLIENT_ID_CONFIG = "client.id";
  public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";
  public static final String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
  public static final String MAX_REQUEST_SIZE_CONFIG = "max.request.size";
  public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
  public static final String RECONNECT_BACKOFF_MAX_MS_CONFIG = "reconnect.backoff.max.ms";
  public static final String MAX_BLOCK_MS_CONFIG = "max.block.ms";
  public static final String BUFFER_MEMORY_CONFIG = "buffer.memory";
  public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
  public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
  public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
  public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
  public static final String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";
  public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
  public static final String MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION =
      "max.in.flight.requests.per.connection";
  public static final String RETRIES_CONFIG = "retries";
  public static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
  public static final String KEY_SERIALIZER_CLASS_DOC =
      "Serializer class for key that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";
  public static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";
  public static final String VALUE_SERIALIZER_CLASS_DOC =
      "Serializer class for value that implements the <code>org.apache.kafka.common.serialization.Serializer</code> interface.";
  public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG =
      "socket.connection.setup.timeout.ms";
  public static final String SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG =
      "socket.connection.setup.timeout.max.ms";
  public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
  public static final String PARTITIONER_CLASS_CONFIG = "partitioner.class";
  public static final String INTERCEPTOR_CLASSES_CONFIG = "interceptor.classes";
  public static final String ENABLE_IDEMPOTENCE_CONFIG = "enable.idempotence";
  public static final String TRANSACTION_TIMEOUT_CONFIG = "transaction.timeout.ms";
  public static final String TRANSACTIONAL_ID_CONFIG = "transactional.id";
  public static final String SECURITY_PROVIDERS_CONFIG = "security.providers";

  public static final List<String> ALL_CONFIGS =
      Utils.constants(ProducerConfigs.class, name -> name.endsWith("CONFIG"));

  // ---------------------------------[values]---------------------------------//
  public static final String COMPRESSION_TYPE_NONE = "none";
  public static final String COMPRESSION_TYPE_GZIP = "gzip";
  public static final String COMPRESSION_TYPE_SNAPPY = "snappy";
  public static final String COMPRESSION_TYPE_LZ4 = "lz4";
  public static final String COMPRESSION_TYPE_ZSTD = "zstd";

  private ProducerConfigs() {}
}
