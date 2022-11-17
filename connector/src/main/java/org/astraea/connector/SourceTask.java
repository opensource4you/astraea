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
import org.apache.kafka.connect.source.SourceRecord;
import org.astraea.common.VersionUtils;
import org.astraea.common.cost.Configuration;
import org.astraea.common.producer.Metadata;
import org.astraea.common.producer.Record;

public abstract class SourceTask extends org.apache.kafka.connect.source.SourceTask {

  protected void init(Configuration configuration) {
    // empty
  }

  protected abstract Collection<Record<byte[], byte[]>> take() throws InterruptedException;

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
    init(Configuration.of(props));
  }

  @Override
  public final List<SourceRecord> poll() throws InterruptedException {
    var records = take();
    if (records == null || records.isEmpty()) return null;
    return records.stream()
        .map(
            r ->
                new SourceRecord(
                    Map.of(),
                    Map.of(),
                    r.topic(),
                    r.partition(),
                    Schema.BYTES_SCHEMA,
                    r.key(),
                    Schema.BYTES_SCHEMA,
                    r.value(),
                    r.timestamp()))
        .collect(Collectors.toList());
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
}
