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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.common.json.JsonConverter;

public class RecordConverter {
  private final List<String> colsName;

  private RecordConverter(List<String> colsName) {
    this.colsName = colsName;
  }

  RecordConverter of(List<String> colsName) {
    return new RecordConverter(colsName);
  }

  Map<String, String> toMap(List<String> row, List<Integer> keysLocation) {
    var key = keysLocation.stream().collect(Collectors.toMap(colsName::get, row::get));
    String jsonKey = JsonConverter.jackson().toJson(key);
    return Map.of(jsonKey, String.join(",", row));
  }
}
