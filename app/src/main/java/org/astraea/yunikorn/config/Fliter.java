package org.astraea.yunikorn.config;

import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Fliter {
  private String type;
  private List<String> users;
  private List<String> groups;
}
