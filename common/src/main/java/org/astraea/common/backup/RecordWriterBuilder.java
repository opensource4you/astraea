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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;
import org.astraea.common.DataSize;
import org.astraea.common.consumer.Record;

public class RecordWriterBuilder {

  private static final Function<OutputStream, RecordWriter> V0 =
      outputStream ->
          new RecordWriter() {
            private final AtomicInteger count = new AtomicInteger();
            private final LongAdder size = new LongAdder();

            @Override
            public void append(Record<byte[], byte[]> record) {
              var topicBytes = record.topic().getBytes(StandardCharsets.UTF_8);
              var recordSize =
                  2 // [topic size 2bytes]
                      + topicBytes.length // [topic]
                      + 4 // [partition 4bytes]
                      + 8 // [offset 8bytes]
                      + 8 // [timestamp 8bytes]
                      + 4 // [key length 4bytes]
                      + (record.key() == null ? 0 : record.key().length) // [key]
                      + 4 // [value length 4bytes]
                      + (record.value() == null ? 0 : record.value().length) // [value]
                      + 4 // [header size 4bytes]
                      + record.headers().stream()
                          .mapToInt(
                              h ->
                                  2 // [header key length 2bytes]
                                      + (h.key() == null
                                          ? 0
                                          : h.key()
                                              .getBytes(StandardCharsets.UTF_8)
                                              .length) // [header key]
                                      + 4 // [header value length 4bytes]
                                      + (h.value() == null ? 0 : h.value().length) // [header value]
                              )
                          .sum();
              size.add(recordSize);
              // TODO reuse the recordBuffer
              var recordBuffer = ByteBuffer.allocate(4 + recordSize);
              recordBuffer.putInt(recordSize);
              ByteUtils.putLengthString(recordBuffer, record.topic());
              recordBuffer.putInt(record.partition());
              recordBuffer.putLong(record.offset());
              recordBuffer.putLong(record.timestamp());
              ByteUtils.putLengthBytes(recordBuffer, record.key());
              ByteUtils.putLengthBytes(recordBuffer, record.value());
              recordBuffer.putInt(record.headers().size());
              record
                  .headers()
                  .forEach(
                      h -> {
                        ByteUtils.putLengthString(recordBuffer, h.key());
                        ByteUtils.putLengthBytes(recordBuffer, h.value());
                      });
              try {
                outputStream.write(recordBuffer.array());
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
              count.incrementAndGet();
            }

            @Override
            public DataSize size() {
              return DataSize.Byte.of(size.sum());
            }

            @Override
            public int count() {
              return count.get();
            }

            @Override
            public void flush() {
              try {
                outputStream.flush();
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }

            @Override
            public void close() {
              try {
                outputStream.write(ByteUtils.of(-1).array());
                outputStream.flush();
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
          };

  public static final short LATEST_VERSION = (short) 0;

  private final short version;
  private OutputStream fs;

  RecordWriterBuilder(short version, OutputStream outputStream) {
    this.version = version;
    this.fs = outputStream;
  }

  public RecordWriterBuilder compression() throws IOException {
    this.fs = new GZIPOutputStream(this.fs);
    return this;
  }

  public RecordWriterBuilder buffered() {
    this.fs = new BufferedOutputStream(this.fs);
    return this;
  }

  public RecordWriterBuilder buffered(int size) {
    this.fs = new BufferedOutputStream(this.fs, size);
    return this;
  }

  public RecordWriter build() {
    try {
      switch (version) {
        case 0:
          fs.write(ByteUtils.toBytes(version));
          return V0.apply(fs);
        default:
          throw new IllegalArgumentException("unsupported version: " + version);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
