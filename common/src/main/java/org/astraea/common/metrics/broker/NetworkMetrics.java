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
package org.astraea.common.metrics.broker;

import java.util.Arrays;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.MBeanClient;
import org.astraea.app.common.EnumInfo;

public class NetworkMetrics {

  public enum Request implements EnumInfo {
    PRODUCE("Produce"),
    FETCH("Fetch"),
    LIST_OFFSETS("ListOffsets"),
    METADATA("Metadata"),
    LEADER_AND_ISR("LeaderAndIsr"),
    STOP_REPLICA("StopReplica"),
    UPDATE_METADATA("UpdateMetadata"),
    CONTROLLED_SHUTDOWN("ControlledShutdown"),
    OFFSET_COMMIT("OffsetCommit"),
    OFFSET_FETCH("OffsetFetch"),
    FIND_COORDINATOR("FindCoordinator"),
    JOIN_GROUP("JoinGroup"),
    HEARTBEAT("Heartbeat"),
    LEAVE_GROUP("LeaveGroup"),
    SYNC_GROUP("SyncGroup"),
    DESCRIBE_GROUPS("DescribeGroups"),
    LIST_GROUPS("ListGroups"),
    SASL_HANDSHAKE("SaslHandshake"),
    API_VERSIONS("ApiVersions"),
    CREATE_TOPICS("CreateTopics"),
    DELETE_TOPICS("DeleteTopics"),
    DELETE_RECORDS("DeleteRecords"),
    INIT_PRODUCER_ID("InitProducerId"),
    OFFSET_FOR_LEADER_EPOCH("OffsetForLeaderEpoch"),
    ADD_PARTITIONS_TO_TXN("AddPartitionsToTxn"),
    ADD_OFFSETS_TO_TXN("AddOffsetsToTxn"),
    END_TXN("EndTxn"),
    WRITE_TXN_MARKERS("WriteTxnMarkers"),
    TXN_OFFSET_COMMIT("TxnOffsetCommit"),
    DESCRIBE_ACLS("DescribeAcls"),
    CREATE_ACLS("CreateAcls"),
    DELETE_ACLS("DeleteAcls"),
    DESCRIBE_CONFIGS("DescribeConfigs"),
    ALTER_CONFIGS("AlterConfigs"),
    ALTER_REPLICA_LOG_DIRS("AlterReplicaLogDirs"),
    DESCRIBE_LOG_DIRS("DescribeLogDirs"),
    SASL_AUTHENTICATE("SaslAuthenticate"),
    CREATE_PARTITIONS("CreatePartitions"),
    CREATE_DELEGATION_TOKEN("CreateDelegationToken"),
    RENEW_DELEGATION_TOKEN("RenewDelegationToken"),
    EXPIRE_DELEGATION_TOKEN("ExpireDelegationToken"),
    DESCRIBE_DELEGATION_TOKEN("DescribeDelegationToken"),
    DELETE_GROUPS("DeleteGroups"),
    ELECT_LEADERS("ElectLeaders"),
    INCREMENTAL_ALTER_CONFIGS("IncrementalAlterConfigs"),
    ALTER_PARTITION_REASSIGNMENTS("AlterPartitionReassignments"),
    LIST_PARTITION_REASSIGNMENTS("ListPartitionReassignments"),
    OFFSET_DELETE("OffsetDelete"),
    DESCRIBE_CLIENT_QUOTAS("DescribeClientQuotas"),
    ALTER_CLIENT_QUOTAS("AlterClientQuotas"),
    DESCRIBE_USER_SCRAM_CREDENTIALS("DescribeUserScramCredentials"),
    ALTER_USER_SCRAM_CREDENTIALS("AlterUserScramCredentials"),
    //    VOTE("Vote"), available for CONTROLLER
    //    BEGIN_QUORUM_EPOCH("BeginQuorumEpoch"), available for CONTROLLER
    //    END_QUORUM_EPOCH("EndQuorumEpoch"), available for CONTROLLER
    //    DESCRIBE_QUORUM("DescribeQuorum"), available for new BROKER and CONTROLLER
    ALTER_PARTITION("AlterPartition"),
    UPDATE_FEATURES("UpdateFeatures"),
    //    ENVELOPE("Envelope"), available for CONTROLLER
    //    FETCH_SNAPSHOT("FetchSnapshot"), available for CONTROLLER
    DESCRIBE_CLUSTER("DescribeCluster"),
    DESCRIBE_PRODUCERS("DescribeProducers"),
    //    BROKER_REGISTRATION("BrokerRegistration"), available for CONTROLLER
    //    BROKER_HEARTBEAT("BrokerHeartbeat"), available for CONTROLLER
    //    UNREGISTER_BROKER("UnregisterBroker"), available for new BROKER and CONTROLLER
    DESCRIBE_TRANSACTIONS("DescribeTransactions"),
    LIST_TRANSACTIONS("ListTransactions"),
    ALLOCATE_PRODUCER_IDS("AllocateProducerIds");

    public static Request ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Request.class, alias);
    }

    private final String metricName;

    Request(String metricName) {
      this.metricName = metricName;
    }

    public String metricName() {
      return metricName;
    }

    @Override
    public String alias() {
      return metricName();
    }

    @Override
    public String toString() {
      return EnumInfo.alias2String(this);
    }

    public Histogram fetch(MBeanClient mBeanClient) {
      return new Histogram(
          mBeanClient.queryBean(
              BeanQuery.builder()
                  .domainName("kafka.network")
                  .property("type", "RequestMetrics")
                  .property("request", this.metricName())
                  .property("name", "TotalTimeMs")
                  .build()));
    }

    public static class Histogram implements HasHistogram {

      private final BeanObject beanObject;

      public Histogram(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public Request type() {
        return Request.of(beanObject.properties().get("request"));
      }

      @Override
      public String toString() {
        return beanObject().toString();
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }

    public static Request of(String metricName) {
      return Arrays.stream(Request.values())
          .filter(metric -> metric.metricName().equalsIgnoreCase(metricName))
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("No such metric: " + metricName));
    }
  }

  private NetworkMetrics() {}
}
