package com.tencent.flink.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ReadHBaseExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, envSettings);

        tableEnv.executeSql(htable());
        tableEnv.executeSql("select rowkey, info.age, info.core, info.id from hTable");
    }

    private static String htable() {
        return "CREATE TABLE hTable (\n"
            + " rowkey STRING,\n"
            + " info ROW<age INT, core INT, id STRING>,\n"
            + " PRIMARY KEY (rowkey) NOT ENFORCED\n"
            + ") WITH (\n"
            + " 'connector' = 'hbase-2.2',\n"
            + " 'table-name' = 'lizy_test',\n"
            + " 'zookeeper.quorum' = '10.0.16.10:2181'\n"
            + ")";
    }

}
