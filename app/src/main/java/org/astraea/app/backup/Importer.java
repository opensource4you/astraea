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
package org.astraea.app.backup;

import com.beust.jcommander.Parameter;
import java.nio.file.Path;
import org.astraea.common.argument.PathField;
import org.astraea.common.backup.RecordReader;
import org.astraea.common.producer.Producer;

public class Importer {

  public static void main(String[] args) {
    var arg = Argument.parse(new Argument(), args);
    System.out.println("prepare to import data from " + arg.input);
    var count = 0;
    try (var producer = Producer.of(arg.bootstrapServers())) {
      var iter = RecordReader.read(arg.input.toFile());
      while (iter.hasNext()) {
        var record = iter.next();
        if (record.key() == null && record.value() == null) continue;
        producer
            .sender()
            .topic(record.topic())
            .partition(record.partition())
            .key(record.key())
            .value(record.value())
            .timestamp(record.timestamp())
            .run();
        count++;
      }
    }
    System.out.println("succeed to import " + count + " records from " + arg.input);
  }

  private static class Argument extends org.astraea.common.argument.Argument {

    @Parameter(
        names = {"--input"},
        description = "Path: the local folder to offer data",
        converter = PathField.class,
        required = true)
    Path input;
  }
}
