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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import org.astraea.common.consumer.Record;

public interface RecordWriter {

  static void write(File file, short version, Iterator<Record<byte[], byte[]>> records) {
    switch (version) {
      case 0:
        try (var writer = new FileOutputStream(file)) {
          var channel = writer.getChannel();
          channel.write(ByteBufferUtils.of(version));
          writeV0(channel, records);
          writer.flush();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        break;
      default:
        throw new IllegalArgumentException("unsupported version: " + version);
    }
  }

  private static void writeV0(WritableByteChannel channel, Iterator<Record<byte[], byte[]>> records)
      throws IOException {
    var count = 0;
    while (records.hasNext()) {
      var record = records.next();
      var topicBytes = record.topic().getBytes(StandardCharsets.UTF_8);
      // [topic size 2bytes][topic][partition 4bytes][offset 8bytes][timestamp 8bytes]
      // [key length 4bytes][key][value length 4bytes][value][header size 4bytes]
      // [header key length 2bytes + header key + header value 4bytes + header value]
      var recordSize =
          2
              + topicBytes.length
              + 4
              + 8
              + 8
              + 4
              + (record.key() == null ? 0 : record.key().length)
              + 4
              + (record.value() == null ? 0 : record.value().length)
              + 4
              + record.headers().stream()
                  .mapToInt(
                      h ->
                          2
                              + (h.key() == null
                                  ? 0
                                  : h.key().getBytes(StandardCharsets.UTF_8).length)
                              + 4
                              + (h.value() == null ? 0 : h.value().length))
                  .sum();
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
      count++;
    }
    channel.write(ByteBufferUtils.of(count));
  }
}
