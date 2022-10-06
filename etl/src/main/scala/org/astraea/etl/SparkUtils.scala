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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, concat, struct, to_json}

object SparkUtils {
  def createSpark(deploymentModel: String): SparkSession = {
    SparkSession
      .builder()
      .master(deploymentModel)
      .appName("Astraea ETL")
      .getOrCreate()
  }

  /** Turn the original DataFrame into a key-value table.Integrate all columns
    * into one value->josh. If there are multiple primary keys, key will become
    * keyA_keyB_....
    *
    * {{{
    * Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
    *
    * // +-------+---------------------------+
    * // |    key|                      value|
    * // +-------+---------------------------+
    * // |Michael|{"name":"Michael","age":29}|
    * // |   Andy|{"name":"Andy","age":30}   |
    * // | Justin|{"name":"Justin","age":19} |
    * // +-------+---------------------------+
    * }}}
    * @param dataFrame
    *   any DataFrame
    * @param pk
    *   primary keys
    * @return
    *   json df
    */
  def csvToJSON(dataFrame: DataFrame, pk: Map[String, String]): DataFrame = {
    dataFrame
      .withColumn("value", to_json(struct($conforms("*"))))
      .withColumn("key", concat(pk.keys.map(col).toSeq: _*))
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
  }
}
