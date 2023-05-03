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
package org.astraea.common.backup;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import org.astraea.common.ByteUtils;
import org.astraea.common.Header;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Record;
import org.astraea.common.generated.RecordOuterClass;

public class RecordReaderBuilder {

  private static final Function<InputStream, RecordReader> V0 =
      inputStream ->
          new RecordReader() {
            private Record<byte[], byte[]> current = null;

            @Override
            public boolean hasNext() {
              // Try to parse a record from the inputStream. And store the parsed record for
              // RecordReader#next().
              if (current == null) current = readRecord(inputStream);

              // nextCache is null if the stream reach EOF.
              return current != null;
            }

            @Override
            public Record<byte[], byte[]> next() {
              if (hasNext()) {
                var next = current;
                current = null;
                return next;
              }
              throw new NoSuchElementException("RecordReader has no more elements.");
            }
          };

  /** Parsed message if successful, or null if the stream is at EOF. */
  private static Record<byte[], byte[]> readRecord(InputStream inputStream) {
    var outerRecord =
        Utils.packException(() -> RecordOuterClass.Record.parseDelimitedFrom(inputStream));
    // inputStream reaches EOF
    if (outerRecord == null) return null;

    return new Record<>() {
      @Override
      public String topic() {
        return outerRecord.getTopic();
      }

      @Override
      public List<Header> headers() {
        return outerRecord.getHeadersList().stream()
            .map(header -> new Header(header.getKey(), header.getValue().toByteArray()))
            .collect(Collectors.toUnmodifiableList());
      }

      @Override
      public byte[] key() {
        return outerRecord.getKey().toByteArray();
      }

      @Override
      public byte[] value() {
        return outerRecord.getValue().toByteArray();
      }

      @Override
      public long offset() {
        return outerRecord.getOffset();
      }

      @Override
      public long timestamp() {
        return outerRecord.getTimestamp();
      }

      @Override
      public int partition() {
        return outerRecord.getPartition();
      }

      @Override
      public int serializedKeySize() {
        return outerRecord.getKey().size();
      }

      @Override
      public int serializedValueSize() {
        return outerRecord.getValue().size();
      }

      @Override
      public Optional<Integer> leaderEpoch() {
        return Optional.empty();
      }
    };
  }

  private InputStream fs;

  RecordReaderBuilder(InputStream inputStream) {
    this.fs = inputStream;
  }

  public RecordReaderBuilder compression() throws IOException {
    this.fs = new GZIPInputStream(this.fs);
    return this;
  }

  public RecordReaderBuilder buffered() {
    this.fs = new BufferedInputStream(this.fs);
    return this;
  }

  public RecordReaderBuilder buffered(int size) {
    this.fs = new BufferedInputStream(this.fs, size);
    return this;
  }

  public RecordReader build() {
    var version = ByteUtils.readShort(fs);
    switch (version) {
      case 0:
        return V0.apply(fs);
      default:
        throw new IllegalArgumentException("unsupported version: " + version);
    }
  }
}
