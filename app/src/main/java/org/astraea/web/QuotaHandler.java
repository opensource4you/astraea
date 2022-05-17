package org.astraea.web;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.astraea.admin.Admin;

public class QuotaHandler implements Handler {

  static final String IP_KEY = "ip";
  static final String CLIENT_ID_KEY = "client-id";
  static final String CONNECTION_RATE_KEY = "connection-rate";
  static final String PRODUCE_RATE_KEY = "produce-rate";
  static final String CONSUME_RATE_KEY = "consume-rate";

  private final Admin admin;

  QuotaHandler(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Quotas get(Optional<String> target, Map<String, String> queries) {
    if (queries.containsKey(IP_KEY))
      return new Quotas(admin.quotas(org.astraea.admin.Quota.Target.IP, queries.get(IP_KEY)));
    if (queries.containsKey(CLIENT_ID_KEY))
      return new Quotas(
          admin.quotas(org.astraea.admin.Quota.Target.CLIENT_ID, queries.get(CLIENT_ID_KEY)));
    return new Quotas(admin.quotas());
  }

  @Override
  public JsonObject post(PostRequest request) {
    if (request.get(IP_KEY).isPresent()) {
      admin
          .quotaCreator()
          .ip(request.value(IP_KEY))
          .connectionRate(request.intValue(CONNECTION_RATE_KEY))
          .create();
      return new Quotas(admin.quotas(org.astraea.admin.Quota.Target.IP, request.value(IP_KEY)));
    }
    if (request.get(CLIENT_ID_KEY).isPresent()) {
      admin
          .quotaCreator()
          .clientId(request.value(CLIENT_ID_KEY))
          .produceRate(request.intValue(PRODUCE_RATE_KEY, Integer.MAX_VALUE))
          .consumeRate(request.intValue(CONSUME_RATE_KEY, Integer.MAX_VALUE))
          .create();
      return new Quotas(
          admin.quotas(org.astraea.admin.Quota.Target.CLIENT_ID, request.value(CLIENT_ID_KEY)));
    }
    return ErrorObject.for404("You must define either " + CLIENT_ID_KEY + " or " + IP_KEY);
  }

  static class Target implements JsonObject {
    final String name;
    final String value;

    Target(String name, String value) {
      this.name = name;
      this.value = value;
    }
  }

  static class Limit implements JsonObject {
    final String name;
    final double value;

    Limit(String name, double value) {
      this.name = name;
      this.value = value;
    }
  }

  static class Quota implements JsonObject {
    final Target target;
    final Limit limit;

    public Quota(org.astraea.admin.Quota quota) {
      this(
          quota.target().nameOfKafka(),
          quota.targetValue(),
          quota.limit().nameOfKafka(),
          quota.limitValue());
    }

    public Quota(String target, String targetValue, String limit, double limitValue) {
      this.target = new Target(target, targetValue);
      this.limit = new Limit(limit, limitValue);
    }
  }

  static class Quotas implements JsonObject {
    final List<Quota> quotas;

    Quotas(Collection<org.astraea.admin.Quota> quotas) {
      this.quotas = quotas.stream().map(Quota::new).collect(Collectors.toUnmodifiableList());
    }
  }
}
