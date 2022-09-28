package org.astraea.etl.KafkaAdmin

trait TopicCreator {
  def topic(name: String): TopicCreator

  def numberOfPartitions(num: Int): TopicCreator

  def numberOfReplicas(num: Short): TopicCreator

  def config(key: String, value: String): TopicCreator

  def config(map: Map[String, String]): TopicCreator

  def create(): Unit
}
