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
      // [topic size 2bytes][topic][partition 4bytes][key length 4bytes][key]
      var recordSize =
          2 + topicBytes.length + 4 + 4 + (record.key() == null ? 0 : record.key().length);
      var recordBuffer = ByteBuffer.allocate(4 + recordSize);
      recordBuffer.putInt(recordSize);
      recordBuffer.putShort((short) topicBytes.length);
      recordBuffer.put(ByteBuffer.wrap(topicBytes));
      recordBuffer.putInt(record.partition());
      recordBuffer.putInt(record.key() == null ? -1 : record.key().length);
      if (record.key() != null) recordBuffer.put(record.key());
      channel.write(recordBuffer.flip());
      count++;
    }
    channel.write(ByteBufferUtils.of(count));
  }

  void write(File file, short version, Iterator<Record<byte[], byte[]>> records, int batchSize);

  void write(
      File file,
      short version,
      Iterator<Record<byte[], byte[]>> records,
      int batchSize,
      String protocol,
      String ip,
      int port);

  void write(
      File file, short version, Iterator<Record<byte[], byte[]>> records, String compression);

  void write(
      File file,
      short version,
      Iterator<Record<byte[], byte[]>> records,
      int batchSize,
      String compression);

  void write(
      File file,
      short version,
      Iterator<Record<byte[], byte[]>> records,
      String compression,
      String protocol,
      String ip,
      int port);

  void write(
      File file,
      short version,
      Iterator<Record<byte[], byte[]>> records,
      String protocol,
      String ip,
      int port);

  /**
   * write compressed data to remote storage with batch
   *
   * @param file the file be written
   * @param version current version
   * @param records the records need to export
   * @param batchSize the byte size of a batch, default is 0
   * @param compression the compression type, default is none
   * @param protocol the protocol for remote storage, like ftp, hdfs or samba
   * @param ip the remote server ip
   * @param port the remote server port
   */
  void write(
      File file,
      short version,
      Iterator<Record<byte[], byte[]>> records,
      int batchSize,
      String compression,
      String protocol,
      String ip,
      int port);
}
