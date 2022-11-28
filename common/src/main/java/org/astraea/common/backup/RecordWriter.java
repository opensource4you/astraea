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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import org.astraea.common.DataSize;
import org.astraea.common.consumer.Record;

public interface RecordWriter extends AutoCloseable {
  void append(Record<byte[], byte[]> record);

  /**
   * @return size of all records
   */
  DataSize size();

  /**
   * @return count of all records
   */
  int count();

  void flush();

  @Override
  void close();

  static RecordWriterBuilder builder(File file) {
    try {
      return builder(new FileOutputStream(file));
    } catch (FileNotFoundException e) {
      throw new UncheckedIOException(e);
    }
  }

  static RecordWriterBuilder builder(OutputStream outputStream) {
    return new RecordWriterBuilder(RecordWriterBuilder.LATEST_VERSION, outputStream);
  }
}
