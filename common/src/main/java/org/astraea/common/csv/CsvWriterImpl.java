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

import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import java.util.List;
import org.astraea.common.Utils;

public class CsvWriterImpl implements CsvWriter {
  private final ICSVWriter csvWriter;
  private int genericLength = -1;

  CsvWriterImpl(CSVWriterBuilder csvWriterBuilder) {
    this.csvWriter = csvWriterBuilder.build();
  }

  @Override
  public void append(List<String> nextLine) {
    if (nextLine == null) throw new RuntimeException("You can't write null list.");
    if (genericLength == -1) genericLength = nextLine.size();
    else if (genericLength != nextLine.size()) {
      if (!String.join("", nextLine).isEmpty())
        throw new RuntimeException(
            "The length of the row:"
                + String.join(",", nextLine)
                + " does not meet the standard. Each row of data should be equal in length.");
    }
    csvWriter.writeNext(nextLine.toArray(new String[0]));
  }

  @Override
  public void rawAppend(List<String> nextLine) {
    csvWriter.writeNext(nextLine.toArray(new String[0]));
  }

  @Override
  public void flush() {
    Utils.packException(csvWriter::flush);
  }

  @Override
  public void close() {
    Utils.packException(csvWriter::close);
  }
}
