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

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, struct, to_json}

class DataFrameOp(dataFrame: DataFrame) {

  /** Turn the original DataFrame into a key-value table.Integrate all columns
    * into one value->josh. If there are multiple primary keys, key will become
    * keyA_keyB_....
    *
    * {{{
    * Seq(Person("Michael","A.K", 29), Person("Andy","B.C", 30), Person("Justin","C.L", 19))
    *
    * Person
    * |-- FirstName: string
    * |-- SecondName: string
    * |-- Age: Integer
    *
    * Key:FirstName,SecondName
    *
    * // +-----------+---------------------------------------------------+
    * // |        key|                                              value|
    * // +-----------+---------------------------------------------------+
    * // |Michael,A.K|{"FirstName":"Michael","SecondName":"A.K","Age":29}|
    * // |   Andy,B.C|{"FirstName":"Andy","SecondName":"B.C","Age":30}   |
    * // | Justin,C.L|{"FirstName":"Justin","SecondName":"C.L","Age":19} |
    * // +-----------+---------------------------------------------------+
    * }}}
    *
    * @param pk
    *   primary keys
    * @return
    *   json df
    */
  //TODO unitized JSON
  def csvToJSON(pk: Seq[String]): DataFrameOp = {
    new DataFrameOp(
      dataFrame
        .withColumn("value", to_json(struct($conforms("*"))))
        .withColumn("key", concat_ws(",", pk.map(col).seq: _*))
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    )
  }

  def dataFrame(): DataFrame = {
    dataFrame
  }
}

object DataFrameOp {
  def empty(): DataFrameOp = {
    null
  }
}
