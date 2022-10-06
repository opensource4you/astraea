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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Test

class SparkUtilsTest {
  @Test def csvToJSONTest(): Unit = {
    val spark = SparkUtils.createSpark("local[2]")

    import spark.implicits._
    case class Person(name: String, age: Long)
    val data =
      Seq(Person("Michael", 29), Person("Andy", 30), Person("Justin", 19))
    implicit val make: Encoder[Person] =
      org.apache.spark.sql.Encoders.kryo[Person]
    val df =
      spark.createDataset(data).map(x => (x.name, x.age)).toDF("name", "age")

    val json = SparkUtils.csvToJSON(df, Map("name" -> "string"))
    val iterator = (0 to 2).iterator
    json
      .collectAsList()
      .forEach(row => {
        val i = iterator.next()
        assert(row(0) equals data(i).name)
        assert(
          row(1) equals s"""{"name":"${data(i).name}","age":${data(i).age}}"""
        )
      })
  }

  @Test def csvToJsonMulKeysTest(): Unit = {
    val spark = SparkUtils.createSpark("local[2]")

    import spark.implicits._
    case class Person(firstName: String, secondName: String, age: Long)
    val data =
      Seq(
        Person("Michael", "A", 29),
        Person("Andy", "B", 30),
        Person("Justin", "C", 19)
      )
    implicit val make: Encoder[Person] =
      org.apache.spark.sql.Encoders.kryo[Person]
    val df =
      spark
        .createDataset(data)
        .map(x => (x.firstName, x.secondName, x.age))
        .toDF("firstName", "secondName", "age")

    val json = SparkUtils.csvToJSON(
      df,
      Map("firstName" -> "string", "secondName" -> "string")
    )
    val iterator = (0 to 2).iterator
    json.show()
    json
      .collectAsList()
      .forEach(row => {
        val i = iterator.next()
        assert(row(0) equals s"${data(i).firstName}${data(i).secondName}")
        assert(
          row(1) equals s"""{"firstName":"${data(
            i
          ).firstName}","secondName":"${data(i).secondName}","age":${data(
            i
          ).age}}"""
        )
      })
  }

  @Test def jsonToByteTest(): Unit = {
    val spark = SparkUtils.createSpark("local[2]")

    var data = Seq(Row(1, "A1", 52, "fghgh", "sfjojs", "zzz", "final", 5))
    (0 to 10000).iterator.foreach(_ =>
      data = data ++ Seq(Row(1, "A1", 52, "fghgh", "sfjojs", "zzz", "final", 5))
    )

    val structType = new StructType()
      .add("ID", "integer")
      .add("name", "string")
      .add("age", "integer")
      .add("xx", "string")
      .add("yy", "string")
      .add("zz", "string")
      .add("f", "string")
      .add("fInt", "integer")

    val df =
      spark.createDataFrame(spark.sparkContext.parallelize(data), structType)

    val json = SparkUtils
      .csvToJSON(df, Map("ID" -> "integer"))
      .withColumn("byte", col("value").cast("Byte"))
      .selectExpr("CAST(byte AS BYTE)")
    val head = json.head()
    assert(json.filter(_ != head).isEmpty)
  }
}
