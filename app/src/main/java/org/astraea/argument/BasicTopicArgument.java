package org.astraea.argument;

import com.beust.jcommander.Parameter;
import java.util.Collections;
import java.util.Set;

/** By contrast to {@link BasicArgument}, this class offers another common option - topics. */
public abstract class BasicTopicArgument extends BasicArgument {
  @Parameter(
      names = {"--topics"},
      description = "Those topics' partitions will get reassigned",
      validateWith = ArgumentUtil.NotEmptyString.class,
      converter = ArgumentUtil.StringSetConverter.class)
  public Set<String> topics = Collections.emptySet();
}
