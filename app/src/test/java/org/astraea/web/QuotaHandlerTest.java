package org.astraea.web;

import java.util.Map;
import java.util.Optional;
import org.astraea.admin.Admin;
import org.astraea.admin.Quota;
import org.astraea.service.RequireBrokerCluster;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class QuotaHandlerTest extends RequireBrokerCluster {

  @Test
  void testCreateQuota() {
    var ip = "192.168.10.11";
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);

      var result =
          Assertions.assertInstanceOf(
              QuotaHandler.Quotas.class,
              handler.post(
                  PostRequest.of(
                      Map.of(QuotaHandler.IP_KEY, ip, QuotaHandler.CONNECTION_RATE_KEY, "10"))));
      Assertions.assertEquals(1, result.quotas.size());
      Assertions.assertEquals(
          Quota.Target.IP.nameOfKafka(), result.quotas.iterator().next().target.name);
      Assertions.assertEquals(ip, result.quotas.iterator().next().target.value);
      Assertions.assertEquals(
          Quota.Limit.IP_CONNECTION_RATE.nameOfKafka(), result.quotas.iterator().next().limit.name);
      Assertions.assertEquals(10, result.quotas.iterator().next().limit.value);
    }
  }

  @Test
  void testQuery() {
    var ip0 = "192.168.10.11";
    var ip1 = "192.168.10.12";
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);

      handler.post(
          PostRequest.of(Map.of(QuotaHandler.IP_KEY, ip0, QuotaHandler.CONNECTION_RATE_KEY, "10")));
      handler.post(
          PostRequest.of(Map.of(QuotaHandler.IP_KEY, ip1, QuotaHandler.CONNECTION_RATE_KEY, "20")));
      Assertions.assertEquals(
          1, handler.get(Optional.empty(), Map.of(QuotaHandler.IP_KEY, ip0)).quotas.size());
      Assertions.assertEquals(
          1, handler.get(Optional.empty(), Map.of(QuotaHandler.IP_KEY, ip1)).quotas.size());
    }
  }

  @Test
  void testQueryNonexistentQuota() {
    var ip = "192.168.10.11";
    try (Admin admin = Admin.of(bootstrapServers())) {
      var handler = new QuotaHandler(admin);
      Assertions.assertEquals(
          0,
          Assertions.assertInstanceOf(
                  QuotaHandler.Quotas.class,
                  handler.get(Optional.empty(), Map.of(Quota.Target.IP.nameOfKafka(), "unknown")))
              .quotas
              .size());

      Assertions.assertEquals(
          0,
          Assertions.assertInstanceOf(
                  QuotaHandler.Quotas.class,
                  handler.get(
                      Optional.empty(), Map.of(Quota.Target.CLIENT_ID.nameOfKafka(), "unknown")))
              .quotas
              .size());
    }
  }
}
