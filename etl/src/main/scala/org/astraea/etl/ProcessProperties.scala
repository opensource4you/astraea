package org.astraea.etl

import java.util.{Calendar, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Using}

object ProcessProperties {
  val default_num_partitions = 15;
  val default_num_replicas = 3;

  //Parameters needed to configure ETL.
  def setConfig(path: String): ConfigurationETL = {
    val properties = readProp(path)
    ConfigurationETL(
      Utils.ToDirectory(properties.getProperty("source.path")),
      Utils.ToDirectory(properties.getProperty("sink.path")),
      //TODO check the format after reading CSV
      properties.getProperty("column.name").split(","),
      properties.getProperty("primary.keys"),
      //TODO check the format after linking Kafka
      properties.getProperty("kafka.bootstrap.servers"),
      isEmptyString(
        properties.getProperty("topic.name"),
        (a: String) => a,
        () =>
          "spark-" + Calendar
            .getInstance()
            .get(Calendar.MILLISECOND) + "-" + Random.nextInt(1000)
      ),
      isEmptyString(
        properties.getProperty("topic.num.partitions"),
        (a: String) => positiveInt(a),
        () => default_num_partitions
      ),
      isEmptyString(
        properties.getProperty("topic.num.replicas"),
        (a: String) => positiveInt(a),
        () => default_num_replicas
      ),
      isEmptyString(
        properties.getProperty("topic.parameters"),
        (a: String) => topicParameters(a),
        () => Option(Map.empty[String, String])
      ).get
    )
  }

  /** Execute the corresponding function according to whether the properties are
   * empty or not.
   *
   * @param str
   * Properties value.
   * @param nonEmpty
   * Function executed when str non-empty.
   * @param empty
   * Function executed when str empty.
   * @tparam C
   * Return type.
   * @return
   * C.
   */
  def isEmptyString[C](
                        str: String,
                        nonEmpty: (String) => C,
                        empty: () => C
                      ): C = {
    if (str.nonEmpty) nonEmpty(str)
    else empty()
  }

  //Handling the topic.parameters parameter.
  def topicParameters(
                       topicParameters: String
                     ): Option[Map[String, String]] = {
    val parameters = topicParameters.split(",")
    var paramArray: ArrayBuffer[Array[String]] = ArrayBuffer()
    for (elem <- parameters) {
      val pm = elem.split(":")
      if (pm.length != 2) {
        throw new IllegalArgumentException(
          "The format of topic parameters is wrong.For example: keyA:valueA,keyB:valueB,keyC:valueC..."
        )
      }
      paramArray = paramArray :+ pm
    }
    Some(paramArray.map { case Array(x, y) => (x, y) }.toMap)
  }

  def readProp(path: String): Properties = {
    val properties = new Properties()
    Using(scala.io.Source.fromFile(path)) { bufferedSource =>
      properties.load(bufferedSource.reader())
    }
    properties
  }

  def positiveInt(num: String): Int = {
    if (!Utils.isPositive(num.toInt)) {
      throw new IllegalArgumentException(
        num + "is a negative number, and it must be a positive number here."
      )
    }
    num.toInt
  }
}
