package com.tencent.flink.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class KafkaToPrintExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.enableCheckpointing(600_000);
        streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        streamEnv.getCheckpointConfig().setCheckpointTimeout(600_000);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, envSettings);

        tableEnv.executeSql(createKafkaTableSQL());
        Table table = tableEnv.sqlQuery(queryKafkaTableSQL());
        DataStream<Tuple2<Boolean, Row>> resultDs = tableEnv.toRetractStream(table, Row.class);
        resultDs.print();

        streamEnv.execute();
    }

    private static String createKafkaTableSQL(){
        return  "CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'lizy_test_demo',\n" +
                "  'properties.bootstrap.servers' = '119.29.50.160:50000',\n" +
                "  'properties.group.id' = 'lizy_consumer_demo_test',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"ckafka-xjvq5jz9#rodenli\" password=\"LZY8023je@\";',\n" +
                "  'format' = 'json'\n" +
                ")";
    }

    private static String queryKafkaTableSQL(){
        return "select * from KafkaTable;";
    }

}
