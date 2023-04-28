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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.astraea.common.EnumInfo;
import org.astraea.common.metrics.BeanObject;
import org.astraea.common.metrics.BeanQuery;
import org.astraea.common.metrics.HasBeanObject;
import org.astraea.common.metrics.MBeanClient;

public final class LogMetrics {
  public static final String DOMAIN_NAME = "kafka.log";
  public static final String LOG_TYPE = "Log";

  public static final Collection<BeanQuery> QUERIES =
      Stream.of(LogCleanerManager.ALL.values().stream(), Log.ALL.values().stream())
          .flatMap(f -> f)
          .collect(Collectors.toUnmodifiableList());

  public enum LogCleanerManager implements EnumInfo {
    UNCLEANABLE_BYTES("uncleanable-bytes"),
    UNCLEANABLE_PARTITIONS_COUNT("uncleanable-partitions-count");

    private static final Map<LogCleanerManager, BeanQuery> ALL =
        Arrays.stream(LogCleanerManager.values())
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    m ->
                        BeanQuery.builder()
                            .domainName(DOMAIN_NAME)
                            .property("type", "LogCleanerManager")
                            .property("logDirectory", "*")
                            .property("name", m.metricName)
                            .build()));

    public static LogCleanerManager ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(LogCleanerManager.class, alias);
    }

    private final String metricName;

    LogCleanerManager(String name) {
      this.metricName = name;
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
      return alias();
    }

    public List<Gauge> fetch(MBeanClient mBeanClient) {
      return mBeanClient.beans(ALL.get(this)).stream()
          .map(Gauge::new)
          .collect(Collectors.toUnmodifiableList());
    }

    public static class Gauge implements HasGauge<Long> {
      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String path() {
        var path = beanObject().properties().get("logDirectory");
        if (path.startsWith("\"")) path = path.substring(1);
        if (path.endsWith("\"")) path = path.substring(0, path.length() - 1);
        return path;
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public LogCleanerManager type() {
        return ofAlias(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }

  public enum Log implements EnumInfo {
    LOG_END_OFFSET("LogEndOffset"),
    LOG_START_OFFSET("LogStartOffset"),
    NUM_LOG_SEGMENTS("NumLogSegments"),
    SIZE("Size");

    private static final Map<Log, BeanQuery> ALL =
        Arrays.stream(Log.values())
            .collect(
                Collectors.toUnmodifiableMap(
                    Function.identity(),
                    m ->
                        BeanQuery.builder()
                            .domainName(DOMAIN_NAME)
                            .property("type", LOG_TYPE)
                            .property("topic", "*")
                            .property("partition", "*")
                            .property("name", m.metricName)
                            // Due to a Kafka bug. This log metrics might come with an `is-future`
                            // property
                            // with it.
                            // And the property is never removed even if the folder migration is
                            // done.
                            // We use the BeanQuery property list pattern to work around this issue.
                            // See https://github.com/apache/kafka/pull/12979
                            .propertyListPattern(true)
                            .build()));

    public static Log ofAlias(String alias) {
      return EnumInfo.ignoreCaseEnum(Log.class, alias);
    }

    private final String metricName;

    Log(String name) {
      this.metricName = name;
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
      return alias();
    }

    public static Collection<Gauge> gauges(Collection<HasBeanObject> beans, Log type) {
      return beans.stream()
          .filter(m -> m instanceof Gauge)
          .map(m -> (Gauge) m)
          .filter(m -> m.type() == type)
          .collect(Collectors.toUnmodifiableList());
    }

    public List<Gauge> fetch(MBeanClient mBeanClient) {
      return mBeanClient.beans(ALL.get(this)).stream()
          .map(Gauge::new)
          .collect(Collectors.toUnmodifiableList());
    }

    public Builder builder() {
      return new Builder(this);
    }

    public static class Builder {
      private final LogMetrics.Log metric;
      private String topic;
      private int partition;
      private long value;

      public Builder(Log metric) {
        this.metric = metric;
      }

      public Builder topic(String topic) {
        this.topic = topic;
        return this;
      }

      public Builder partition(int partition) {
        this.partition = partition;
        return this;
      }

      public Builder logSize(long value) {
        this.value = value;
        return this;
      }

      public Gauge build() {
        return new Gauge(
            new BeanObject(
                LogMetrics.DOMAIN_NAME,
                Map.ofEntries(
                    Map.entry("type", LOG_TYPE),
                    Map.entry("name", metric.metricName()),
                    Map.entry("topic", topic),
                    Map.entry("partition", String.valueOf(partition))),
                Map.of("Value", value)));
      }
    }

    public static class Gauge implements HasGauge<Long> {
      private final BeanObject beanObject;

      public Gauge(BeanObject beanObject) {
        this.beanObject = beanObject;
      }

      public String topic() {
        return beanObject().properties().get("topic");
      }

      public int partition() {
        return Integer.parseInt(beanObject().properties().get("partition"));
      }

      public String metricsName() {
        return beanObject().properties().get("name");
      }

      public Log type() {
        return ofAlias(metricsName());
      }

      @Override
      public BeanObject beanObject() {
        return beanObject;
      }
    }
  }

  private LogMetrics() {}
}
