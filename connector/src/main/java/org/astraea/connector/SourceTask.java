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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.astraea.common.Configuration;
import org.astraea.common.VersionUtils;
import org.astraea.common.producer.Metadata;
import org.astraea.common.producer.Record;

public abstract class SourceTask extends org.apache.kafka.connect.source.SourceTask {

  protected abstract void init(Configuration configuration, MetadataStorage storage);

  /**
   * use {@link Record#builder()} or {@link SourceRecord#builder()} to construct the returned
   * records
   */
  protected abstract Collection<SourceRecord> take() throws InterruptedException;

  protected void commit(Metadata metadata) throws InterruptedException {
    // empty
  }

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
    init(new Configuration(props), MetadataStorage.of(context.offsetStorageReader()));
  }

  @Override
  public final List<org.apache.kafka.connect.source.SourceRecord> poll()
      throws InterruptedException {
    var records = take();
    if (records == null || records.isEmpty()) return null;
    return records.stream()
        .map(
            r ->
                new org.apache.kafka.connect.source.SourceRecord(
                    r.metadataIndex(),
                    r.metadata(),
                    r.topic(),
                    r.partition().orElse(null),
                    r.key() == null ? null : Schema.BYTES_SCHEMA,
                    r.key(),
                    r.value() == null ? null : Schema.BYTES_SCHEMA,
                    r.value(),
                    r.timestamp().orElse(null),
                    r.headers().stream()
                        .map(h -> new HeaderImpl(h.key(), null, h.value()))
                        .collect(Collectors.toList())))
        .toList();
  }

  @Override
  public final void stop() {
    close();
  }

  @Override
  public final void commitRecord(
      org.apache.kafka.connect.source.SourceRecord record,
      org.apache.kafka.clients.producer.RecordMetadata metadata)
      throws InterruptedException {
    commit(Metadata.of(metadata));
  }

  private record HeaderImpl(String key, Schema schema, Object value)
      implements org.apache.kafka.connect.header.Header {

    @Override
    public org.apache.kafka.connect.header.Header with(Schema schema, Object value) {
      return new HeaderImpl(key, schema, value);
    }

    @Override
    public org.apache.kafka.connect.header.Header rename(String key) {
      return new HeaderImpl(key, schema, value);
    }
  }
}
