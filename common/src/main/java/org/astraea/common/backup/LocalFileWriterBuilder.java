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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;
import org.astraea.common.consumer.Record;

public class LocalFileWriterBuilder {

  private OutputStream fs;

  public LocalFileWriterBuilder(File file) throws FileNotFoundException {
    this.fs = new FileOutputStream(file);
  }

  public LocalFileWriterBuilder compression() throws IOException {
    this.fs = new GZIPOutputStream(this.fs);
    return this;
  }

  public LocalFileWriterBuilder buffered() {
    this.fs = new BufferedOutputStream(this.fs);
    return this;
  }

  public LocalFileWriterBuilder buffered(int size) {
    this.fs = new BufferedOutputStream(this.fs, size);
    return this;
  }

  public RecordWriter build() {
    return new LocalFileWriterImpl(this);
  }

  private static class LocalFileWriterImpl implements RecordWriter {

    private OutputStream fs;

    @Override
    public void append(Iterator<Record<byte[], byte[]>> records) {
      System.out.println("append in local file");
    }

    @Override
    public void flush() throws IOException {
      this.fs.flush();
    }

    private LocalFileWriterImpl(LocalFileWriterBuilder builder) {
      this.fs = builder.fs;
    }
  }
}
