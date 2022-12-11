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
package org.astraea.connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import org.astraea.common.Configuration;
import org.astraea.common.Utils;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.producer.Record;
import org.astraea.fs.ftp.FtpFileSystem;

public class FtpSourceTask extends SourceTask {
  private FtpFileSystem fs;
  private LinkedList<String> path;

  protected void init(Configuration configuration) {
    this.fs = new FtpFileSystem(configuration);
    this.path = new LinkedList<>();
    Arrays.asList(configuration.requireString("input").split(","))
        .forEach(input -> path.addAll(fs.listFiles(input)));
  }

  @Override
  protected Collection<Record<byte[], byte[]>> take() {
    if (!path.isEmpty()) {
      var records = new ArrayList<Record<byte[], byte[]>>();
      var current = path.poll();
      var inputStream = fs.read(current);
      var reader = RecordReader.builder(inputStream).build();
      while (reader.hasNext()) {
        var record = reader.next();
        if (record.key() == null && record.value() == null) continue;
        records.add(
            Record.builder()
                .topic(record.topic())
                .partition(record.partition())
                .key(record.key())
                .value(record.value())
                .timestamp(record.timestamp())
                .headers(record.headers())
                .build());
      }
      System.out.println("succeed to add " + records.size() + " records from " + current);
      Utils.packException(inputStream::close);
      return records;
    }
    return null;
  }

  @Override
  protected void close() {
    this.fs.close();
  }
}
