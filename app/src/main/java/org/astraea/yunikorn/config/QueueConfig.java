package org.astraea.yunikorn.config;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class QueueConfig {
  private String name;
  private Boolean parent;
  private Resources resources;
  private Map<String, String> properties;
  private long maxapplications;
  private String adminacl;
  private String submitacl;
  private ChildTemplate childtemplate;
  private List<QueueConfig> queues;
  private List<Limit> limits;
}
