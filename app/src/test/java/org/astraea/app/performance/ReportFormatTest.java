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
package org.astraea.app.performance;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ReportFormatTest {
  @Test
  void testInitCSV() throws IOException {
    var stringWriter = new StringWriter();
    var writer = new BufferedWriter(stringWriter);
    var elements = List.of(titleValue("t1", "1"), titleValue("t2", "2"));

    ReportFormat.initCSVFormat(writer, elements);
    writer.flush();

    Assertions.assertEquals("t1, t2, \n", stringWriter.toString());
  }

  @Test
  void testLogToCSV() throws IOException, InterruptedException {
    StringWriter stringWriter = new StringWriter();
    var writer = new BufferedWriter(stringWriter);
    var elements = List.of(titleValue("t1", "1"), titleValue("t2", "2"));

    ReportFormat.logToCSV(writer, elements);
    writer.flush();

    Assertions.assertEquals("1, 2, \n", stringWriter.toString());
  }

  private static ReportFormat.CSVContentElement titleValue(String title, String value) {
    return ReportFormat.CSVContentElement.create(title, () -> value);
  }
}
