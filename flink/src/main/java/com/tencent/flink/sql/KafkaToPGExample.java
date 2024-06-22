package com.tencent.flink.sql;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToPGExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.enableCheckpointing(600_000);
        streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        streamEnv.getCheckpointConfig().setCheckpointTimeout(600_000);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, envSettings);

        tableEnv.executeSql(createKafkaTableSQL());
        tableEnv.executeSql("drop table if exists pg_table");
        tableEnv.executeSql(createPGTableSQL());
        tableEnv.executeSql(insertPGSQL());
    }

    private static String createKafkaTableSQL() {
        return "CREATE TABLE kafka_table (\n" +
            "  `id` BIGINT,\n" +
            "  `name` STRING,\n" +
            "  `age` INTEGER,\n" +
            "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'lizy_test_demo',\n" +
            "  'properties.bootstrap.servers' = 'ip:host',\n" +
            "  'properties.group.id' = 'lizy_consumer_demo_test',\n" +
            "  'scan.startup.mode' = 'latest-offset',\n" +
            "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
            "  'properties.sasl.mechanism' = 'PLAIN',\n" +
            "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"xxx\" password=\"xxx\";',\n"
            +
            "  'format' = 'json'\n" +
            ")";
    }

    private static String createPGTableSQL() {
        return "CREATE TABLE pg_table (\n" +
            "  `id` BIGINT,\n" +
            "  `name` STRING,\n" +
            "  `age` INTEGER,\n" +
            "  `ts` TIMESTAMP(3)\n" +
            ") WITH (" +
            "  'connector' = 'jdbcPG',\n" +
            "  'url' = 'jdbc:postgresql://ip:host/rongyao?currentSchema=public&reWriteBatchedInserts=true',\n"
            +
            "  'username' = 'dbadmin',\n" +
            "  'password' = 'xxx',\n" +
            "  'table-name' = 'user_info',\n" +
            "  'sink.buffer-flush.max-rows' = '5000',\n" +
            "  'sink.buffer-flush.interval' = '10s' ,\n" +
            "  'batch.delete.size' = '512',\n" +
            "  'write-mode' = 'copy');";
    }

    private static String insertPGSQL(){
        return "insert into pg_table(id, name, age, ts)"
            + "select id, name, age, ts "
            + "from kafka_table;";
    }
}
