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
package org.astraea.etl

object spark2kafka {

  def main(args: Array[String]): Unit = {
    val metaData = Metadata(Utils.requireFile(args(0)))
    val spark = CSVReader.createSpark(metaData.deploymentModel)
    val userSchema =
      CSVReader.createSchema(metaData.column, metaData.primaryKeys)
    val csvDF =
      CSVReader.readCSV(spark, userSchema, metaData.sourcePath.getPath)
    //TODO How to start
    CSVReader.writeKafka(csvDF, metaData).start().awaitTermination()
  }
}
