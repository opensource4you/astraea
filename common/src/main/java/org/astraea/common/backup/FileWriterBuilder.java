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
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
import org.astraea.common.consumer.Record;

public class FileWriterBuilder {

  private OutputStream fs;
  private short version = 0;

  public FileWriterBuilder compression() throws IOException {
    this.fs = new GZIPOutputStream(this.fs);
    return this;
  }

  public FileWriterBuilder buffered() {
    this.fs = new BufferedOutputStream(this.fs);
    return this;
  }

  public FileWriterBuilder buffered(int size) {
    this.fs = new BufferedOutputStream(this.fs, size);
    return this;
  }

  public FileWriterBuilder version(short version) {
    this.version = version;
    return this;
  }

  public FileWriterBuilder output(OutputStream outputStream) {
    this.fs = outputStream;
    return this;
  }

  public RecordWriter build() throws IOException {
    return new FileWriterImpl(this);
  }

  private static class FileWriterImpl implements RecordWriter {
    private final OutputStream fs;
    private final WritableByteChannel channel;
    int recordCnt;
    short version;

    @Override
    public void append(Record<byte[], byte[]> record) throws IOException {
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
                                  : h.key().getBytes(StandardCharsets.UTF_8).length) // [header key]
                              + 4 // [header value length 4bytes]
                              + (h.value() == null ? 0 : h.value().length) // [header value]
                      )
                  .sum();
      // TODO reuse the recordBuffer
      var recordBuffer = ByteBuffer.allocate(4 + recordSize);
      recordBuffer.putInt(recordSize);
      ByteBufferUtils.putLengthString(recordBuffer, record.topic());
      recordBuffer.putInt(record.partition());
      recordBuffer.putLong(record.offset());
      recordBuffer.putLong(record.timestamp());
      ByteBufferUtils.putLengthBytes(recordBuffer, record.key());
      ByteBufferUtils.putLengthBytes(recordBuffer, record.value());
      recordBuffer.putInt(record.headers().size());
      record
          .headers()
          .forEach(
              h -> {
                ByteBufferUtils.putLengthString(recordBuffer, h.key());
                ByteBufferUtils.putLengthBytes(recordBuffer, h.value());
              });
      channel.write(recordBuffer.flip());
      recordCnt++;
    }

    @Override
    public void flush() throws IOException {
      this.fs.flush();
    }

    @Override
    public void close() throws Exception {
      channel.write(ByteBufferUtils.of(recordCnt));
      fs.flush();
    }

    private FileWriterImpl(FileWriterBuilder builder) throws IOException {
      this.fs = builder.fs;
      this.version = builder.version;
      this.channel = Channels.newChannel(this.fs);
      this.recordCnt = 0;

      channel.write(ByteBufferUtils.of(version));
    }
  }
}
