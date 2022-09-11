package org.astraea.etl

import java.io.File

/** Parameters required for Astraea ETL.
 *
 * @param sourcePath
 * The data source path should be a directory.
 * @param sinkPath
 * The data sink path should be a directory.
 * @param columnName
 * The CSV Column Name.For example:stringA,stringB,stringC...
 * @param primaryKeys
 * Primary keys.
 * @param kafkaBootstrapServers
 * The Kafka bootstrap servers.
 * @param topicName
 * Set your topic name, if it is empty that will set it to "'spark'-'current
 * time by hourDay'-'random number'".
 * @param numPartitions
 * Set the number of topic partitions, if it is empty that will set it to 15.
 * @param numReplicas
 * Set the number of topic replicas, if it is empty that will set it to 3.
 * @param topicParameters
 * The rest of the topic can be configured parameters.For example:
 * keyA:valueA,keyB:valueB,keyC:valueC...
 */
case class ConfigurationETL(
                             sourcePath: File,
                             sinkPath: File,
                             columnName: Array[String],
                             primaryKeys: String,
                             kafkaBootstrapServers: String,
                             topicName: String,
                             numPartitions: Int,
                             numReplicas: Int,
                             topicParameters: Map[String, String]
                           )
