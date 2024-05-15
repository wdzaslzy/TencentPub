package com.tencent.flink.ry;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ReadKafkaToPrintBug {

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
        tableEnv.executeSql(createPrintSQL());
//        tableEnv.executeSql("insert into pgcdw_public_ceshi_public_demo_kafka2pg_test_concurrent_update select * from kafka_pg_rao2_kafka_pg_rao2;");
        tableEnv.executeSql(insertSQL());

//        Table table = tableEnv.sqlQuery("select * from kafka_pg_rao2_kafka_pg_rao2");
//        DataStream<Tuple2<Boolean, Row>> resultDs = tableEnv.toRetractStream(table, Row.class);
//        resultDs.print();


        streamEnv.execute();
    }

    private static String createKafkaTableSQL() {
        return "CREATE TABLE kafka_pg_rao2_kafka_pg_rao2 (\n"
            + "     data_date                              string comment '日期年月日'\n"
            + "    ,data_time                              string comment '时分秒'\n"
            + "    ,launch_type                       string comment '启动方式'\n"
            + "    ,dl_type                           string comment '下载类型'\n"
            + "    ,client_version_name               string comment '客户端版本'\n"
            + "    ,app_package                       string comment '包名'\n"
            + "    ,app_type                          string comment '应用类型'\n"
            + "    ,dl_page_code                      string comment '页面名'\n"
            + "    ,assembly_type                     string comment '组件类型'\n"
            + "    ,assembly_name                     string comment '组件名'\n"
            + "    ,assembly_pos                      string comment '组件位置'\n"
            + "    ,adunit_id                         string comment '广告位(ID)'\n"
            + "    ,ad_id                             string comment '广告(ID)'\n"
            + "    ,media_id                          string comment '媒体(ID)'\n"
            + "    ,down_succ_user_qty                bigint comment '下载完成人数'\n"
            + "    ,down_succ_qty                     bigint comment '下载完成次数'\n"
            + "    ,accu_down_succ_user_qty_1d        bigint comment '当日累计下载完成人数'\n"
            + "    ,accu_down_succ_qty_1d             bigint comment '当日累计下载完成次数'\n"
            + ") WITH (\n"
            + "'connector' = 'kafka',\n"
            + "'format' = 'json',\n"
            + "'topic' = 'dm_adplatform_ad_play_link_inc_s',\n"
            + "'properties.bootstrap.servers' = '******',\n" +
            "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
            "  'properties.sasl.mechanism' = 'PLAIN',\n" +
            "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"******\" password=\"******\";',\n"
            + "'properties.group.id' = 'test_lizy',\n"
            + "'scan.startup.mode' = 'latest-offset');";
    }

    private static String createPrintSQL() {
        return "CREATE TABLE pgcdw_public_ceshi_public_demo_kafka2pg_test_concurrent_update (\n"
            + "     data_date                         string comment '日期年月日'\n"
            + "    ,data_time                         string comment '时分秒'\n"
            + "    ,launch_type                       string comment '启动方式'\n"
            + "    ,dl_type                           string comment '下载类型'\n"
            + "    ,client_version_name               string comment '客户端版本'\n"
            + "    ,app_package                       string comment '包名'\n"
            + "    ,app_type                          string comment '应用类型'\n"
            + "    ,dl_page_code                      string comment '页面名'\n"
            + "    ,assembly_type                     string comment '组件类型'\n"
            + "    ,assembly_name                     string comment '组件名'\n"
            + "    ,assembly_pos                      string comment '组件位置'\n"
            + "    ,adunit_id                         string comment '广告位(ID)'\n"
            + "    ,ad_id                             string comment '广告(ID)'\n"
            + "    ,media_id                          string comment '媒体(ID)'\n"
            + "    ,down_succ_user_qty                bigint comment '下载完成人数'\n"
            + "    ,down_succ_qty                     bigint comment '下载完成次数'\n"
            + "    ,accu_down_succ_user_qty_1d        bigint comment '当日累计下载完成人数'\n"
            + "    ,accu_down_succ_qty_1d             bigint comment '当日累计下载完成次数'\n"
            + "    ,primary key(data_date,data_time,launch_type,dl_type,client_version_name,app_package,app_type,dl_page_code,assembly_type,assembly_name,assembly_pos,adunit_id,ad_id,media_id) not enforced\n"
            + ") WITH (\n"
            + "'connector' = 'print'\n"
            + ");";
    }

    private static String insertSQL() {
        return "insert into pgcdw_public_ceshi_public_demo_kafka2pg_test_concurrent_update\n"
            + "select\n"
            + "     data_date\n"
            + "    ,if(data_time              is null or char_length(data_time          ) = 0,'null_value',data_time           )\n"
            + "    ,if(launch_type            is null or char_length(launch_type        ) = 0,'null_value',launch_type         )\n"
            + "    ,if(dl_type                is null or char_length(dl_type            ) = 0,'null_value',dl_type             )\n"
            + "    ,ifclient_version_name\n"
//            + "    ,if(client_version_name is null or char_length(client_version_name) = 0,'null_value',client_version_name)\n"
            + "    ,if(app_package            is null or char_length(app_package        ) = 0,'null_value',app_package         )\n"
            + "    ,if(app_type               is null or char_length(app_type           ) = 0,'null_value',app_type            )\n"
            + "    ,if(dl_page_code           is null or char_length(dl_page_code       ) = 0,'null_value',dl_page_code        )\n"
            + "    ,if(assembly_type          is null or char_length(assembly_type      ) = 0,'null_value',assembly_type       )\n"
            + "    ,if(assembly_name          is null or char_length(assembly_name      ) = 0,'null_value',assembly_name       )\n"
            + "    ,if(assembly_pos           is null or char_length(assembly_pos       ) = 0,'null_value',assembly_pos        )\n"
            + "    ,if(adunit_id              is null or char_length(adunit_id          ) = 0,'null_value',adunit_id           )\n"
            + "    ,if(ad_id                  is null or char_length(ad_id              ) = 0,'null_value',ad_id               )\n"
            + "    ,if(media_id               is null or char_length(media_id           ) = 0,'null_value',media_id            )\n"
            + "    ,ifnull(down_succ_user_qty         ,0)\n"
            + "    ,ifnull(down_succ_qty              ,0)\n"
            + "    ,ifnull(accu_down_succ_user_qty_1d ,0)\n"
            + "    ,ifnull(accu_down_succ_qty_1d      ,0)\n"
            + "from kafka_pg_rao2_kafka_pg_rao2\n"
            + ";";
    }


}
