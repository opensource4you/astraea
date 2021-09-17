package org.astraea.metrics.jmx;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.management.*;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

public class TestUtility {

  static MBeanServer setupMBeanServerSpy(Map<ObjectName, Object> mbeans) {
    MBeanServer mBeanServer = MBeanServerFactory.createMBeanServer();
    for (ObjectName objectName : mbeans.keySet()) {
      try {
        mBeanServer.registerMBean(mbeans.get(objectName), objectName);
      } catch (InstanceAlreadyExistsException
          | MBeanRegistrationException
          | NotCompliantMBeanException e) {
        throw new RuntimeException(e);
      }
    }
    return mBeanServer;
  }

  static JMXConnectorServer setupJmxServerSpy(Map<ObjectName, Object> mbeans) {
    JMXServiceURL serviceURL;
    try {
      serviceURL = new JMXServiceURL("service:jmx:rmi://127.0.0.1");
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }

    MBeanServer mBeanServerSpy = setupMBeanServerSpy(mbeans);
    try {
      return JMXConnectorServerFactory.newJMXConnectorServer(serviceURL, null, mBeanServerSpy);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

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

  public interface OneAttributeMBean<T> {
    void setValue(T newValue);

    T getValue();
  }

  public interface TwoAttributeMBean<T1, T2> {
    void setValue1(T1 newValue);

    T1 getValue1();

    void setValue2(T2 newValue);

    T2 getValue2();
  }

  static class OneAttribute<T> implements OneAttributeMBean<T> {

    T value;

    @Override
    public void setValue(T newValue) {
      value = newValue;
    }

    @Override
    public T getValue() {
      return value;
    }

    public OneAttribute(T value) {
      this.value = value;
    }
  }

  static class TwoAttribute<T1, T2> implements TwoAttributeMBean<T1, T2> {

    T1 value1;
    T2 value2;

    public TwoAttribute(T1 value1, T2 value2) {
      this.value1 = value1;
      this.value2 = value2;
    }

    @Override
    public void setValue1(T1 newValue) {
      value1 = newValue;
    }

    @Override
    public T1 getValue1() {
      return value1;
    }

    @Override
    public void setValue2(T2 newValue) {
      value2 = newValue;
    }

    @Override
    public T2 getValue2() {
      return value2;
    }
  }

  public static class MBeansDataset {

    public static final ObjectName objectName1 = createObjectName("org.example", "type", "object1");
    public static final ObjectName objectName2 = createObjectName("org.example", "type", "object2");
    public static final ObjectName objectName3 = createObjectName("org.astraea", "type", "object3");
    public static final ObjectName objectName4 = createObjectName("org.astraea", "type", "object4");

    public static final Map<ObjectName, Object> mbeansServerContent =
        Map.of(
            objectName1, new OneAttribute<String>("String1"),
            objectName2, new OneAttribute<String>("String2"),
            objectName3, new TwoAttribute<String, Integer>("String3", 3),
            objectName4, new TwoAttribute<String, Integer>("String4", 4));

    public static BeanObject asBeanObject(ObjectName name) {
      return new BeanObject(name.getDomain(), name.getKeyPropertyList(), Collections.emptyMap());
    }

    private static ObjectName createObjectName(String domain, String key, String value) {
      try {
        return ObjectName.getInstance(domain, key, value);
      } catch (MalformedObjectNameException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
