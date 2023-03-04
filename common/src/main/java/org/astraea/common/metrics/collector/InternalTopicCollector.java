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
package org.astraea.common.metrics.collector;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Stream;
import org.astraea.common.admin.ClusterBean;
import org.astraea.common.metrics.HasBeanObject;

public class InternalTopicCollector implements MetricCollector {
  private InternalTopicCollector(String bootstrapServer) {
    //
  }

  @Override
  public Collection<MetricSensor> metricSensors() {
    return null;
  }

  @Override
  public Set<Integer> listIdentities() {
    return null;
  }

  @Override
  public Set<Class<? extends HasBeanObject>> listMetricTypes() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public Stream<HasBeanObject> metrics() {
    return null;
  }

  @Override
  public ClusterBean clusterBean() {
    return null;
  }

  @Override
  public void close() {}

  public static class Builder {
    private String bootstrapServer = "";
    private int threads = 1;

    public Builder bootstrapServer(String address) {
      this.bootstrapServer = address;
      return this;
    }

    public Builder threads(int threads){
      this.threads = threads;
      return this;
    }

    public InternalTopicCollector build() {
      if (bootstrapServer.isEmpty())
        throw new IllegalArgumentException(
            "Bootstrap server is required for building InternalTopicCollector.");
      return new InternalTopicCollector(bootstrapServer, threads);
    }
  }
}
