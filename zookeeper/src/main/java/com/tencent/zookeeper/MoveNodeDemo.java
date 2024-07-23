package com.tencent.zookeeper;

import java.io.IOException;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooKeeper;

public class MoveNodeDemo {

    public static void main(String[] args) throws IOException {
        String zkServers = args[0];
        String oldPath = args[1];
        String newPath = args[2];

        ZkClient zkClient = new ZkClient(zkServers, 60000, 5000);

        Object data = zkClient.readData(oldPath);

        zkClient.createPersistent(newPath, data);

        zkClient.close();
    }

    private static ZooKeeper connectZkCluster()
        throws IOException {
        return new ZooKeeper(
            "192.168.1.3:2181,192.168.1.4:2181,192.168.1.5:2181",
            20000,
            watchedEvent -> {
                // 发生变更的节点路径
                String path = watchedEvent.getPath();
                System.out.println("path:" + path);

                // 通知状态
                Event.KeeperState state = watchedEvent.getState();
                System.out.println("KeeperState:" + state);

                // 事件类型
                Event.EventType type = watchedEvent.getType();
                System.out.println("EventType:" + type);
            }
        );
    }

}
