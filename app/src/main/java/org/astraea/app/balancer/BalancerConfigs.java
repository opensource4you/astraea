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
package org.astraea.app.balancer;

import com.beust.jcommander.Parameter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.management.remote.JMXServiceURL;
import org.astraea.app.argument.ClassFields;
import org.astraea.app.balancer.executor.RebalancePlanExecutor;
import org.astraea.app.balancer.executor.StraightPlanExecutor;
import org.astraea.app.balancer.generator.RebalancePlanGenerator;
import org.astraea.app.balancer.generator.ShufflePlanGenerator;
import org.astraea.app.balancer.metrics.JmxMetricSampler;
import org.astraea.app.balancer.metrics.MetricSource;
import org.astraea.common.Utils;
import org.astraea.common.argument.Argument;
import org.astraea.common.argument.Field;
import org.astraea.common.cost.CostFunction;
import org.astraea.common.cost.ReplicaLeaderCost;

public class BalancerConfigs extends Argument {

  @Parameter(
      names = {"--jmx.servers"},
      description = "The JMX service URL for for each broker in the cluster",
      converter = BrokerJMXServiceURLConverter.class,
      required = true)
  public Map<Integer, JMXServiceURL> jmxServers;

  @Parameter(
      names = {"--ignored.topics"},
      description = "Topics to ignore")
  public List<String> ignoredTopics = List.of("__consumer_offsets");

  @Parameter(
      names = {"--metric.sources"},
      converter = ClassFields.SingleClassField.class)
  public Class<? extends MetricSource> metricSourceClass = JmxMetricSampler.class;

  @Parameter(
      names = {"--cost.functions"},
      converter = ClassFields.ClassesField.class)
  public List<Class<? extends CostFunction>> costFunctionClasses = List.of(ReplicaLeaderCost.class);

  @Parameter(
      names = {"--rebalance.plan.generator"},
      converter = ClassFields.SingleClassField.class)
  public Class<? extends RebalancePlanGenerator> rebalancePlanGeneratorClass =
      ShufflePlanGenerator.class;

  @Parameter(
      names = {"--rebalance.plan.executor"},
      converter = ClassFields.SingleClassField.class)
  public Class<? extends RebalancePlanExecutor> rebalancePlanExecutorClass =
      StraightPlanExecutor.class;

  @Parameter(
      names = {"--rebalance.plan.iteration"},
      converter = ClassFields.SingleClassField.class)
  public int planSearchingIteration = 10000;

  public static class BrokerJMXServiceURLConverter extends Field<Map<Integer, JMXServiceURL>> {

    @Override
    public Map<Integer, JMXServiceURL> convert(String value) {
      return Arrays.stream(value.split(","))
          .map(entry -> entry.split("@"))
          .collect(
              Collectors.toUnmodifiableMap(
                  pair -> Integer.parseInt(pair[0]),
                  pair -> Utils.packException(() -> new JMXServiceURL(pair[1]))));
    }
  }
}
