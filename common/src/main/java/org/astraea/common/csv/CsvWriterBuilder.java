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
import java.io.Writer;
import java.util.List;
import org.astraea.common.Utils;

/** Construct CSVWriterBuilder so that we can use its build pattern. */
public class CsvWriterBuilder {
  private final CSVWriterBuilder csvWriterBuilder;
  private boolean blankLine;

  CsvWriterBuilder(Writer sink) {
    this.csvWriterBuilder = new CSVWriterBuilder(sink);
  }

  public CsvWriterBuilder withLineEnd(String string) {
    this.csvWriterBuilder.withLineEnd(string);
    return this;
  }

  public CsvWriterBuilder blankLine(boolean blankLine) {
    this.blankLine = blankLine;
    return this;
  }

  public CsvWriter build() {
    return new CsvWriterImpl(csvWriterBuilder, blankLine);
  }

  private static class CsvWriterImpl implements CsvWriter {
    private final ICSVWriter csvWriter;
    private final boolean blankLine;
    private int genericLength = -1;

    private CsvWriterImpl(CSVWriterBuilder csvWriterBuilder, boolean blankLine) {
      this.blankLine = blankLine;
      this.csvWriter = csvWriterBuilder.build();
    }

    @Override
    public void append(List<String> nextLine) {
      if (nextLine == null) throw new RuntimeException("You can't write null list.");
      if (genericLength == -1) genericLength = nextLine.size();
      else if (genericLength != nextLine.size()) {
        if (blankLine && String.join("", nextLine).isBlank()) {
          rawAppend(nextLine);
          return;
        }
        throw new RuntimeException(
            "The length of the row:"
                + String.join(",", nextLine)
                + " does not meet the standard. Each row of data should be equal in length.");
      }
      rawAppend(nextLine);
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
}
