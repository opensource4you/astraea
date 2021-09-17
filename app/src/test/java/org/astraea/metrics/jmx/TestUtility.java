package org.astraea.metrics.jmx;

import java.util.HashMap;
import java.util.Hashtable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class TestUtility {

  static String getDomainName(BeanObject object) throws MalformedObjectNameException {
    return ObjectName.getInstance(object.objectName()).getDomain();
  }

  static String getDomainName(BeanQuery object) throws MalformedObjectNameException {
    return ObjectName.getInstance(object.queryString()).getDomain();
  }

  static HashMap<String, String> getPropertyList(BeanObject object)
      throws MalformedObjectNameException {
    Hashtable<String, String> keyPropertyList =
        ObjectName.getInstance(object.objectName()).getKeyPropertyList();
    HashMap<String, String> hm = new HashMap<>();
    for (String s : keyPropertyList.keySet()) {
      hm.put(s, keyPropertyList.get(s));
    }
    return hm;
  }

  static HashMap<String, String> getPropertyList(BeanQuery object)
      throws MalformedObjectNameException {
    Hashtable<String, String> keyPropertyList =
        ObjectName.getInstance(object.queryString()).getKeyPropertyList();
    HashMap<String, String> hm = new HashMap<>();
    for (String s : keyPropertyList.keySet()) {
      hm.put(s, keyPropertyList.get(s));
    }
    return hm;
  }
}
