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
package org.astraea.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.astraea.common.Configuration;
import org.astraea.common.Header;
import org.astraea.common.VersionUtils;
import org.astraea.common.consumer.Record;

public abstract class SinkTask extends org.apache.kafka.connect.sink.SinkTask {

  protected void init(Configuration configuration) {
    // empty
  }

  protected abstract void put(List<Record<byte[], byte[]>> records);

  protected void close() {
    // empty
  }

  // -------------------------[final]-------------------------//
  @Override
  public final String version() {
    return VersionUtils.VERSION;
  }

  @Override
  public final void start(Map<String, String> props) {
    init(new Configuration(props));
  }

  @Override
  public final void put(Collection<SinkRecord> records) {
    if (records != null && !records.isEmpty())
      put(records.stream().map(SinkTask::toRecord).toList());
  }

  private static byte[] toBytes(Schema schema, Object value) {
    if (schema != null && schema.type() != Schema.Type.BYTES)
      throw new IllegalStateException(
          "the allowed schema is Schema.Type.BYTES, but actual schema is " + schema);

    if (value != null && !(value instanceof byte[]))
      throw new DataException(
          "Astraea connector is not compatible with objects of type " + value.getClass());

    return (byte[]) value;
  }

  private static Record<byte[], byte[]> toRecord(SinkRecord record) {
    var key = toBytes(record.keySchema(), record.key());
    var value = toBytes(record.valueSchema(), record.value());
    return Record.builder()
        .topic(record.topic())
        .headers(toHeaders(record.headers()))
        .key(key)
        .value(value)
        .offset(record.kafkaOffset())
        .timestamp(record.timestamp())
        .partition(record.kafkaPartition())
        .serializedKeySize(key == null ? 0 : key.length)
        .serializedValueSize(value == null ? 0 : value.length)
        .build();
  }

  private static List<Header> toHeaders(Headers headers) {
    if (headers == null || headers.isEmpty()) return List.of();
    var hs = new ArrayList<Header>(headers.size());
    headers
        .iterator()
        .forEachRemaining(h -> hs.add(new Header(h.key(), toBytes(h.schema(), h.value()))));
    return Collections.unmodifiableList(hs);
  }

  @Override
  public final void stop() {
    close();
  }
}
