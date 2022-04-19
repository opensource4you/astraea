package org.astraea.yunikorn.metrics.Infos;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Application {
  private String applicationID;

  private String partition;
  private String queueName;
  private long submissionTime;
  private long finishedTime;
  private String applicationState;

  public Application(
      String applicationID,
      String partition,
      String queueName,
      long submissionTime,
      long finishedTime,
      String applicationState) {
    this.applicationID = applicationID;
    this.partition = partition;
    this.queueName = queueName;
    this.submissionTime = submissionTime;
    this.finishedTime = finishedTime;
    this.applicationState = applicationState;
  }
}
