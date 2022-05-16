package org.astraea.admin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.kafka.common.quota.ClientQuotaEntity;

public class Quota {

  static Collection<Quota> of(Map<ClientQuotaEntity, Map<String, Double>> data) {
    return data.entrySet().stream()
        .flatMap(
            clientQuotaEntityMapEntry ->
                clientQuotaEntityMapEntry.getKey().entries().entrySet().stream()
                    .flatMap(
                        stringStringEntry ->
                            clientQuotaEntityMapEntry.getValue().entrySet().stream()
                                .map(
                                    v ->
                                        new Quota(
                                            target(stringStringEntry.getKey()),
                                            stringStringEntry.getValue(),
                                            action(v.getKey()),
                                            v.getValue()))))
        .collect(Collectors.toUnmodifiableList());
  }

  static Target target(String value) {
    return Arrays.stream(Target.values())
        .filter(p -> p.nameOfKafka.equalsIgnoreCase(value))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("unknown: " + value));
  }

  static Action action(String value) {
    return Arrays.stream(Action.values())
        .filter(p -> p.nameOfKafka.equalsIgnoreCase(value))
        .findFirst()
        .orElseThrow(() -> new RuntimeException("unknown: " + value));
  }

  enum Target {
    USER(ClientQuotaEntity.USER),
    CLIENT_ID(ClientQuotaEntity.CLIENT_ID),
    IP(ClientQuotaEntity.IP);
    private final String nameOfKafka;

    Target(String nameOfKafka) {
      this.nameOfKafka = nameOfKafka;
    }

    String nameOfKafka() {
      return nameOfKafka;
    }
  }

  enum Action {
    PRODUCER_BYTE_RATE("producer_byte_rate"),
    CONSUMER_BYTE_RATE("consumer_byte_rate"),
    REQUEST_PERCENTAGE("request_percentage"),
    CONTROLLER_MUTATION_RATE("controller_mutation_rate"),
    IP_CONNECTION_RATE("connection_creation_rate");
    private final String nameOfKafka;

    Action(String nameOfKafka) {
      this.nameOfKafka = nameOfKafka;
    }

    String nameOfKafka() {
      return nameOfKafka;
    }
  }

  private final Target target;

  private final String targetValue;
  private final Action action;
  private final double actionValue;

  public Quota(Target target, String targetValue, Action action, double actionValue) {
    this.target = target;
    this.targetValue = targetValue;
    this.action = action;
    this.actionValue = actionValue;
  }

  public Target target() {
    return target;
  }

  public String targetValue() {
    return targetValue;
  }

  public Action action() {
    return action;
  }

  public double actionValue() {
    return actionValue;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Quota quota = (Quota) o;
    return Double.compare(quota.actionValue, actionValue) == 0
        && target == quota.target
        && Objects.equals(targetValue, quota.targetValue)
        && action == quota.action;
  }

  @Override
  public int hashCode() {
    return Objects.hash(target, targetValue, action, actionValue);
  }

  @Override
  public String toString() {
    return "Quota{"
        + "target="
        + target
        + ", targetValue='"
        + targetValue
        + '\''
        + ", action="
        + action
        + ", actionValue="
        + actionValue
        + '}';
  }
}
