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
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.astraea.common.Utils;
import org.astraea.common.argument.PathField;
import org.astraea.common.argument.StringSetField;
import org.astraea.common.backup.RecordWriter;
import org.astraea.common.consumer.Consumer;
import org.astraea.common.consumer.ConsumerConfigs;
import org.astraea.common.consumer.IteratorLimit;

public class Exporter {

  public static void main(String[] args) {
    var arg = Argument.parse(new Argument(), args);
    var iter =
        Consumer.forTopics(arg.topics)
            .bootstrapServers(arg.bootstrapServers())
            .config(
                ConsumerConfigs.AUTO_OFFSET_RESET_CONFIG,
                ConsumerConfigs.AUTO_OFFSET_RESET_EARLIEST)
            .config(ConsumerConfigs.GROUP_ID_CONFIG, arg.group)
            .iterator(List.of(IteratorLimit.idle(Duration.ofSeconds(3))));
    System.out.println("prepare to export data from " + arg.topics);
    RecordWriter.write(arg.output.toFile(), (short) 0, iter);
    System.out.println("succeed to export data from " + arg.topics);
  }

  private static class Argument extends org.astraea.common.argument.Argument {

    @Parameter(
        names = {"--topics"},
        description = "List<String>: topic names to export data",
        validateWith = StringSetField.class,
        listConverter = StringSetField.class,
        required = true)
    Set<String> topics;

    @Parameter(
        names = {"--output"},
        description = "Path: the local folder to save data",
        converter = PathField.class,
        required = true)
    Path output;

    @Parameter(
        names = {"--group"},
        description =
            "String: the group id used by this exporter. You can run multiples exporter with same id in parallel")
    String group = Utils.randomString();
  }
}
