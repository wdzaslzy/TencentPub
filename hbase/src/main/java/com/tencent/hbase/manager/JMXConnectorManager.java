package com.tencent.hbase.manager;

import java.io.IOException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXConnectorManager {

  public static JMXConnector createJMXConnector(String host, Integer port) throws IOException {

    String url = "service:jmx:rmi:///jndi/rmi://" + host + ":" + port + "/jmxrmi";
    return JMXConnectorFactory.connect(new JMXServiceURL(url));
  }

}
