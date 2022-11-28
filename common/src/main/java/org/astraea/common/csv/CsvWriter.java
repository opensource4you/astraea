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
package org.astraea.common.csv;

import java.io.Writer;
import java.util.List;

public interface CsvWriter extends AutoCloseable {
  /**
   * create CsvWriter builder
   *
   * @param sink target csv writer
   * @return CsvWriterBuilder
   */
  static CsvWriterBuilder builder(Writer sink) {
    return new CsvWriterBuilder(sink);
  }
  /**
   * Writes the next line to the file.Empty fields cannot be written and the lengths of the strings
   * written should be equal.
   *
   * @param nextLine A List<String> with each comma-separated element as a separate entry.
   */
  void append(List<String> nextLine);

  /**
   * Writes the next line without checking.
   *
   * @param nextLine A List<String> with each comma-separated element as a separate entry.
   */
  void rawAppend(List<String> nextLine);

  void flush();

  void close();
}
