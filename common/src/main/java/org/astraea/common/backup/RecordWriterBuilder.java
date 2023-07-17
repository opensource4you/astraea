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
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.zip.GZIPOutputStream;
import org.astraea.common.ByteUtils;
import org.astraea.common.Configuration;
import org.astraea.common.DataSize;
import org.astraea.common.Utils;
import org.astraea.common.consumer.Record;
import org.astraea.common.function.Bi3Function;
import org.astraea.common.generated.RecordOuterClass;

public class RecordWriterBuilder {

  static RecordOuterClass.Record record2Builder(Record<byte[], byte[]> record) {
    return Utils.packException(
        () ->
            RecordOuterClass.Record.newBuilder()
                .setTopic(record.topic())
                .setPartition(record.partition())
                .setOffset(record.offset())
                .setTimestamp(record.timestamp())
                .setKey(record.key() == null ? ByteString.EMPTY : ByteString.copyFrom(record.key()))
                .setValue(
                    record.value() == null ? ByteString.EMPTY : ByteString.copyFrom(record.value()))
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
                        .toList())
                .build());
  }

  private static final Function<OutputStream, RecordWriter> V0 =
      outputStream ->
          new RecordWriter() {
            private final AtomicInteger count = new AtomicInteger();
            private final LongAdder size = new LongAdder();

            private final AtomicLong latestAppendTimestamp = new AtomicLong();

            @Override
            public void append(Record<byte[], byte[]> record) {
              Utils.packException(
                  () -> {
                    var recordBuilder = record2Builder(record);
                    recordBuilder.writeDelimitedTo(outputStream);
                    this.size.add(recordBuilder.getSerializedSize());
                  });
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

  private static final Bi3Function<Configuration, Record, OutputStream, RecordWriter> V1 =
      (configuration, record, outputStream) ->
          new RecordWriter() {
            private final AtomicInteger count = new AtomicInteger();
            private final LongAdder size = new LongAdder();
            private final AtomicLong latestAppendTimestamp = new AtomicLong();
            private final String connectorName;
            private final Long interval;
            private final String compressionType;
            private OutputStream targetOutputStream;

            // instance initializer block
            {
              this.connectorName = configuration.requireString("connector.name");
              this.interval =
                  configuration
                      .string("roll.duration")
                      .map(Utils::toDuration)
                      .orElse(Duration.ofSeconds(3))
                      .toMillis();
              this.compressionType = configuration.string("compression.type").orElse("none");

              switch (this.compressionType) {
                case "gzip" -> Utils.packException(
                    () -> this.targetOutputStream = new GZIPOutputStream(outputStream));
                case "none" -> this.targetOutputStream = outputStream;
                default -> throw new IllegalArgumentException(
                    String.format("compression type '%s' is not supported", this.compressionType));
              }
            }

            byte[] extendString(String input, int length) {
              byte[] original = input.getBytes();
              byte[] result = new byte[length];
              System.arraycopy(original, 0, result, 0, original.length);
              for (int i = original.length; i < result.length; i++) {
                result[i] = (byte) ' ';
              }
              return result;
            }

            void appendMetadata() {
              Utils.packException(
                  () -> {
                    if (this.compressionType.equals("gzip")) {
                      ((GZIPOutputStream) targetOutputStream).finish();
                    }

                    // 552 Bytes total for whole metadata.
                    outputStream.write(
                        this.extendString(
                            this.connectorName, 255)); // 255 Bytes for this connector name
                    outputStream.write(
                        this.extendString(record.topic(), 255)); // 255 Bytes for topic name
                    // 42 Bytes
                    outputStream.write(
                        ByteUtils.toBytes(record.partition())); // 4 Bytes for partition
                    outputStream.write(
                        ByteUtils.toBytes(record.offset())); // 8 bytes for 1st record offset
                    outputStream.write(
                        ByteUtils.toBytes(record.timestamp())); // 8 bytes for 1st record timestamp
                    outputStream.write(ByteUtils.toBytes(this.count())); // 4 Bytes for count
                    outputStream.write(
                        ByteUtils.toBytes(this.interval)); // 8 Bytes for mills of roll.duration
                    outputStream.write(
                        this.extendString(
                            this.compressionType, 10)); // 10 Bytes for compression type name.
                  });
            }

            @Override
            public void append(Record<byte[], byte[]> record) {
              Utils.packException(
                  () -> {
                    var recordBuilder = record2Builder(record);
                    recordBuilder.writeDelimitedTo(this.targetOutputStream);
                    this.size.add(recordBuilder.getSerializedSize());
                  });
              count.incrementAndGet();
              this.latestAppendTimestamp.set(System.currentTimeMillis());
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
            public long latestAppendTimestamp() {
              return this.latestAppendTimestamp.get();
            }

            @Override
            public void close() {
              Utils.packException(
                  () -> {
                    appendMetadata();
                    outputStream.flush();
                    outputStream.close();
                  });
            }
          };

  public static final short LATEST_VERSION = (short) 1;

  private final short version;
  private OutputStream fs;
  private Configuration configuration;
  private Record record;

  RecordWriterBuilder(short version, OutputStream outputStream) {
    this.version = version;
    this.fs = outputStream;
  }

  RecordWriterBuilder(
      short version, OutputStream outputStream, Configuration configuration, Record record) {
    this.version = version;
    this.fs = outputStream;
    this.configuration = configuration;
    this.record = record;
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
          } else if (version == 1) {
            fs.write(ByteUtils.toBytes(version));
            return V1.apply(this.configuration, this.record, fs);
          }
          throw new IllegalArgumentException("unsupported version: " + version);
        });
  }
}
