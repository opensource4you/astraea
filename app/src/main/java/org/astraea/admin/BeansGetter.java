package org.astraea.admin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.astraea.metrics.HasBeanObject;

/** Used to get beanObject using a variety of different keys . */
public interface BeansGetter {
  static BeansGetter of(Map<Integer, Collection<HasBeanObject>> allBeans) {
    return new BeansGetter() {
      @Override
      public Map<Integer, Collection<HasBeanObject>> broker() {
        return Collections.unmodifiableMap(allBeans);
      }

      @Override
      public Collection<HasBeanObject> brokerId(int brokerId) {
        return allBeans.getOrDefault(brokerId, List.of());
      }

      @Override
      public Map<TopicPartition, HasBeanObject> partition(int brokerId) {
        return allBeans.entrySet().stream()
            .filter(entry -> entry.getKey() == brokerId)
            .flatMap(
                entry ->
                    entry.getValue().stream()
                        .filter(
                            x ->
                                x.beanObject().getProperties().containsKey("topic")
                                    && x.beanObject().getProperties().containsKey("partition"))
                        .map(
                            hasBeanObject -> {
                              var properties = hasBeanObject.beanObject().getProperties();
                              var topic = properties.get("topic");
                              var partition = properties.get("partition");
                              return Map.entry(TopicPartition.of(topic, partition), hasBeanObject);
                            }))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }

      @Override
      public Map<TopicPartitionReplica, HasBeanObject> replica() {
        return allBeans.entrySet().stream()
            .flatMap(
                entry ->
                    entry.getValue().stream()
                        .filter(
                            x ->
                                x.beanObject().getProperties().containsKey("topic")
                                    && x.beanObject().getProperties().containsKey("partition"))
                        .map(
                            hasBeanObject -> {
                              var properties = hasBeanObject.beanObject().getProperties();
                              var topic = properties.get("topic");
                              var partition = Integer.parseInt(properties.get("partition"));
                              return Map.entry(
                                  new TopicPartitionReplica(topic, partition, entry.getKey()),
                                  hasBeanObject);
                            }))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
      }
    };
  }

  Map<Integer, Collection<HasBeanObject>> broker();

  Collection<HasBeanObject> brokerId(int brokerId);

  Map<TopicPartition, HasBeanObject> partition(int brokerId);

  Map<TopicPartitionReplica, HasBeanObject> replica();
}
