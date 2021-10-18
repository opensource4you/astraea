package org.astraea.performance;

import com.beust.jcommander.Parameter;
import org.astraea.argument.ArgumentUtil;
import org.astraea.argument.BasicArgument;

public class PerformanceArgument extends BasicArgument {

  @Parameter(
      names = {"--topic"},
      description = "String: topic name",
      validateWith = ArgumentUtil.NotEmptyString.class)
  String topic = "testPerformance-" + System.currentTimeMillis();

  @Parameter(
      names = {"--partitions"},
      description = "Integer: number of partitions to create the topic",
      validateWith = ArgumentUtil.PositiveLong.class)
  int partitions = 1;

  @Parameter(
      names = {"--replicas"},
      description = "Integer: number of replica to create the topic",
      validateWith = ArgumentUtil.PositiveLong.class,
      converter = ArgumentUtil.ShortConverter.class)
  short replicationFactor = 1;

  @Parameter(
      names = {"--producers"},
      description = "Integer: number of producers to produce records",
      validateWith = ArgumentUtil.PositiveLong.class)
  int producers = 1;

  @Parameter(
      names = {"--consumers"},
      description = "Integer: number of consumers to consume records",
      validateWith = ArgumentUtil.NonNegativeLong.class)
  int consumers = 1;

  @Parameter(
      names = {"--records"},
      description = "Integer: number of records to send",
      validateWith = ArgumentUtil.NonNegativeLong.class)
  long records = 1000;

  @Parameter(
      names = {"--record.size"},
      description = "Integer: size of each record",
      validateWith = ArgumentUtil.PositiveLong.class)
  int recordSize = 1024;

  @Parameter(
      names = {"--prop.file"},
      description = "String: path to the properties file",
      validateWith = ArgumentUtil.NotEmptyString.class)
  String propFile;
}
