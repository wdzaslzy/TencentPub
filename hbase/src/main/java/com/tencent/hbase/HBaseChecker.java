package com.tencent.hbase;

import com.tencent.hbase.manager.JMXConnectorManager;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.Set;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;

public class HBaseChecker {

    public static void main(String[] args) {


    }

    /**
     * 检查HBase是否触发Full GC
     */
    private static void checkFullGC() {
        String hostname = "localhost";
        int port = 9010; // JMX 连接端口

        JMXConnector jmxConnector = null;
        try {
            jmxConnector = JMXConnectorManager.createJMXConnector(hostname, port);
            MBeanServerConnection mbeanConnection = jmxConnector.getMBeanServerConnection();

            // 获取所有的 MemoryMXBean
            Set<ObjectName> memoryMXBeans = mbeanConnection.queryNames(
                new ObjectName(ManagementFactory.MEMORY_MXBEAN_NAME), null);

            // 遍历每个 MemoryMXBean
            for (ObjectName memoryMXBeanName : memoryMXBeans) {
                MemoryMXBean memoryMXBean = ManagementFactory.newPlatformMXBeanProxy(
                    mbeanConnection, memoryMXBeanName.toString(), MemoryMXBean.class);

                // 获取堆内存使用情况
                MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
                System.out.println("Heap Memory Usage: " + heapMemoryUsage);

                // 获取非堆内存使用情况
                MemoryUsage nonHeapMemoryUsage = memoryMXBean.getNonHeapMemoryUsage();
                System.out.println("Non-Heap Memory Usage: " + nonHeapMemoryUsage);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jmxConnector != null) {
                try {
                    jmxConnector.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }


}
