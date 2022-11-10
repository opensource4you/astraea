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
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.astraea.common.Utils;

public class OwnCsvWriterBuilder {
  private final CSVWriterBuilder csvWriterBuilder;

  /**
   * Construct CSVWriterBuilder so that we can use its build pattern.
   *
   * @param sink The reader to an underlying CSV source.
   */
  public OwnCsvWriterBuilder(File sink) {
    this.csvWriterBuilder =
        new CSVWriterBuilder(Utils.packException(() -> new FileWriter(sink, true)));
  }

  public OwnCsvWriter build() {
    return new OwnCsvWriter(csvWriterBuilder.build());
  }

  public static class OwnCsvWriter implements Closeable {
    private final ICSVWriter csvWriter;

    private OwnCsvWriter(ICSVWriter csvWriter) {
      this.csvWriter = csvWriter;
    }

    /**
     * Writes the next line to the file.
     *
     * @param nextLine A string array with each comma-separated element as a separate entry.
     */
    public void writeNext(List<String> nextLine) {
      csvWriter.writeNext(nextLine.toArray(new String[0]));
    }

    public void flush() {
      Utils.packException(csvWriter::flush);
    }

    @Override
    public void close() throws IOException {
      csvWriter.close();
    }
  }
}
