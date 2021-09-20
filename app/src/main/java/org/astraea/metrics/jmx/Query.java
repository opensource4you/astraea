package org.astraea.metrics.jmx;

import java.util.Map;
import javax.management.ObjectName;

/**
 * A wrapper interface for {@link ObjectName}
 *
 * <p>{@link MBeanClient} use this interface to get the corresponding {@link ObjectName}
 *
 * <p>So why we don't just use {@link ObjectName}? why we use this interface after all? There are a
 * couple of reason:
 *
 * <ol>
 *   <li>{@link ObjectName} provide various constructors, but non of them are handy. For example you
 *       have to specify query properties by {@link java.util.Hashtable}, which is a deprecated
 *       collection class. Also if you really want to use the full potential of {@link ObjectName}
 *       (property list pattern), the only option is construct the {@link ObjectName} by string,
 *       which is a cripple way to declare what your intent is.
 *   <li>Library user can take advantage of this interface, to implement their own Domain Specific
 *       Language(DSL) upon this interface. All they have to do is provide any {@link Query}
 *       implementation that suits their business domain need. This makes the {@link MBeanClient}
 *       more useful since now it can support various flexible query syntax.
 * </ol>
 */
public interface Query {
  ObjectName getQuery();

  String domainName();

  Map<String, String> properties();
}
