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
package org.astraea.common.connector.impl;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.astraea.common.VersionUtils;

public class TestTextSourceTask extends SourceTask {

  private String topics;

  @Override
  public void start(Map<String, String> props) {
    topics = props.get("topics");
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    var jsonValue = "{\"testKey\":\"testValue\"}";
    var sourceRecord =
        new SourceRecord(
            Map.of(), Map.of(), topics, null, jsonValue.getBytes(StandardCharsets.UTF_8));
    Thread.sleep(5000);
    return List.of(sourceRecord);
  }

  @Override
  public void stop() {}

  @Override
  public String version() {
    return VersionUtils.VERSION;
  }
}
