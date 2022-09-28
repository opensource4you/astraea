package org.astraea.etl.KafkaAdmin

import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.common.config.ConfigResource

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

import scala.xml.Utility

class TopicCreatorImpl(admin: Admin) extends TopicCreator {
  private [this] var topic:String = ""
  private [this] var numberOfPartitions: Int = 1
  private [this] var numberOfReplicas:Short = 1
  private [this] var configs: Map[String, String] = Map.empty[String, String]

  override def topic(name: String): TopicCreator = {
    this.topic = name
    this
  }

  override def numberOfPartitions(num: Int): TopicCreator = {
    this.numberOfPartitions = num
    this
  }

  override def numberOfReplicas(num: Short): TopicCreator = {
    this.numberOfReplicas = num
    this
  }

  override def config(key: String, value: String): TopicCreator = {
    this.configs += (key -> value)
    this
  }

  override def config(map: Map[String, String]): TopicCreator = {
    this.configs = map ++ this.configs
    this
  }

  override def create(): Unit = {
    if (admin.listTopics().names().get().contains(topic)){
      val topicPartitions = admin.describeTopics(ListBuffer(topic).asJava).topicNameValues().get(topic).get().partitions()

      if (!topicPartitions.size().equals(numberOfPartitions)) throw
        new IllegalArgumentException(s"$topic is existent but its partitions:${topicPartitions.size()} is not equal to expected $numberOfPartitions")

      topicPartitions
        .forEach(topic=>if(!topic.replicas().size().equals(numberOfReplicas)){
          throw new IllegalArgumentException(s"$topic is existent but its replicas:${topic.replicas().size()} is not equal to expected $numberOfReplicas")
        })

      val actualConfigs = admin.describeConfigs(List(new ConfigResource(ConfigResource.Type.TOPIC, topic)).asJava).values().get(ConfigResource.Type.TOPIC).get()

      configs.foreach(
        config=>if(!Option(actualConfigs.get(config._1)).exists(actual => actual.value().equals(config._2))){
          throw new IllegalArgumentException(s"$topic is existent but its config:<${config._1}, ${actualConfigs.get(config._1).value()}> is not equal to expected <${config._1},${config._2}>")
        }
      )
      // ok, the existent topic is totally equal to what we want to create.
    }else{
      admin.createTopics(List(new NewTopic(topic, numberOfPartitions, numberOfReplicas).configs(configs.asJava)).asJava).all().get()
    }
  }
}

object TopicCreatorImpl {
  def apply(admin: Admin): TopicCreatorImpl = {
    new TopicCreatorImpl(admin)
  }
}
