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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPOutputStream;
import org.astraea.common.consumer.Record;

public class FtpWriterBuilder {

  private String ip;

  private Integer port;

  private OutputStream fs;

  public FtpWriterBuilder(String ip, int port) throws FileNotFoundException {
    this.ip = ip;
    this.port = port;
  }

  public FtpWriterBuilder compression() throws IOException {
    this.fs = new GZIPOutputStream(this.fs);
    return this;
  }

  public FtpWriterBuilder buffered() {
    this.fs = new BufferedOutputStream(this.fs);
    return this;
  }

  public FtpWriterBuilder buffered(int size) {
    this.fs = new BufferedOutputStream(this.fs, size);
    return this;
  }

  public RecordWriter build() {
    return new FtpWriterImpl(this);
  }

  private static class FtpWriterImpl implements RecordWriter {

    private String ip;
    private Integer port;

    private OutputStream fs;

    public Map<String, Integer> getAddress() {
      if (this.ip == null) {
        return null;
      }
      return Map.of(this.ip, this.port);
    }

    @Override
    public void append(Record<byte[], byte[]> records) {
      System.out.println("append in ftp");
    }

    @Override
    public void flush() throws IOException {
      this.fs.flush();
    }

    private FtpWriterImpl(FtpWriterBuilder builder) {
      this.ip = builder.ip;
      this.port = builder.port;
      this.fs = builder.fs;
    }

    @Override
    public void close() throws Exception {

    }
  }
}
