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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import org.astraea.common.Header;
import org.astraea.common.consumer.Record;

public class RecordReaderBuilder {

  private static final Function<InputStream, RecordReader> V0 =
      inputStream ->
          new RecordReader() {
            private final BufferedInputStream fs;
            private int recordCnt;

            {
              try {
                fs = new BufferedInputStream(inputStream);
                fs.mark(fs.available());
                fs.skip(fs.available() - Integer.BYTES);
                recordCnt = ByteUtils.readInt(fs);
                fs.reset();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public boolean hasNext() {
              return recordCnt > 0;
            }

            @Override
            public Record<byte[], byte[]> next() {
              recordCnt--;
              var recordSize = ByteUtils.readInt(fs);
              var recordBuffer = ByteBuffer.allocate(recordSize);
              int actualSize;
              try {
                actualSize = fs.read(recordBuffer.array());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              if (actualSize != recordSize)
                throw new IllegalStateException(
                    "expected size is " + recordSize + ", but actual size is " + actualSize);
              var topic = ByteUtils.readString(recordBuffer, recordBuffer.getShort());
              var partition = recordBuffer.getInt();
              var offset = recordBuffer.getLong();
              var timestamp = recordBuffer.getLong();
              var key = ByteUtils.readBytes(recordBuffer, recordBuffer.getInt());
              var value = ByteUtils.readBytes(recordBuffer, recordBuffer.getInt());
              var headerCnt = recordBuffer.getInt();
              var headers = new ArrayList<Header>(headerCnt);
              for (int headerIndex = 0; headerIndex < headerCnt; headerIndex++) {
                var headerKey = ByteUtils.readString(recordBuffer, recordBuffer.getShort());
                var headerValue = ByteUtils.readBytes(recordBuffer, recordBuffer.getInt());
                headers.add(Header.of(headerKey, headerValue));
              }

              return Record.builder()
                  .topic(topic)
                  .partition(partition)
                  .offset(offset)
                  .timestamp(timestamp)
                  .key(key)
                  .value(value)
                  .serializedKeySize(key == null ? 0 : key.length)
                  .serializedValueSize(value == null ? 0 : value.length)
                  .headers(headers)
                  .build();
            }
          };

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
