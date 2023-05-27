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

import com.google.protobuf.ByteString;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import org.astraea.common.ByteUtils;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Record;
import org.astraea.common.generated.RecordOuterClass;

public class RecordWriterBuilder {

  private static final Function<OutputStream, RecordWriter> V0 =
      outputStream ->
          new RecordWriter() {
            private final AtomicInteger count = new AtomicInteger();
            private final LongAdder size = new LongAdder();

            private final AtomicLong latestAppendTimestamp = new AtomicLong();

            @Override
            public void append(Record<byte[], byte[]> record) {
              Utils.packException(
                  () ->
                      RecordOuterClass.Record.newBuilder()
                          .setTopic(record.topic())
                          .setPartition(record.partition())
                          .setOffset(record.offset())
                          .setTimestamp(record.timestamp())
                          .setKey(
                              record.key() == null
                                  ? ByteString.EMPTY
                                  : ByteString.copyFrom(record.key()))
                          .setValue(
                              record.value() == null
                                  ? ByteString.EMPTY
                                  : ByteString.copyFrom(record.value()))
                          .addAllHeaders(
                              record.headers().stream()
                                  .map(
                                      header ->
                                          RecordOuterClass.Record.Header.newBuilder()
                                              .setKey(header.key())
                                              .setValue(
                                                  header.value() == null
                                                      ? ByteString.EMPTY
                                                      : ByteString.copyFrom(header.value()))
                                              .build())
                                  .collect(Collectors.toUnmodifiableList()))
                          .build()
                          .writeDelimitedTo(outputStream));
              count.incrementAndGet();
              this.latestAppendTimestamp.set(System.currentTimeMillis());
            }

            @Override
            public long latestAppendTimestamp() {
              return this.latestAppendTimestamp.get();
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
              Utils.packException(outputStream::flush);
            }

            @Override
            public void close() {
              Utils.packException(
                  () -> {
                    outputStream.flush();
                    outputStream.close();
                  });
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
    return Utils.packException(
        () -> {
          if (version == 0) {
            fs.write(ByteUtils.toBytes(version));
            return V0.apply(fs);
          }
          throw new IllegalArgumentException("unsupported version: " + version);
        });
  }
}
