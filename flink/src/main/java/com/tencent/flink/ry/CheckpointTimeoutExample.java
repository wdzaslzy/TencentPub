package com.tencent.flink.ry;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CheckpointTimeoutExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.enableCheckpointing(600_000);
        streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        streamEnv.getCheckpointConfig().setCheckpointTimeout(600_000);
        EnvironmentSettings envSettings = EnvironmentSettings.newInstance().inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, envSettings);

        tableEnv.executeSql(createKafkaTableSQL());
        tableEnv.executeSql(createPGTableSQL());
        tableEnv.executeSql(insertSQL());
    }

    private static String createKafkaTableSQL() {
        return "CREATE TABLE dm_adplatform_ad_play_link_inc_s_kafka\n"
            + "(\n"
            + "    pt_d                    string,\n"
            + "    pt_h                    string,\n"
            + "    pt_m                    string,\n"
            + "    mediaid                 string,\n"
            + "    adunitid                string,\n"
            + "    billingtype             string,\n"
            + "    advertiserid            string,\n"
            + "    advertisertype          string,\n"
            + "    adgroupid               string,\n"
            + "    adcreativeid            string,\n"
            + "    promotionpurpose        string,\n"
            + "    ad_mixranking_sum_m     bigint,\n"
            + "    ad_mixranking_sum_r     bigint,\n"
            + "    ad_mixranking_avg_aptr  float,\n"
            + "    ad_mixranking_sum_fi    bigint,\n"
            + "    ad_mixranking_sum_fu    bigint,\n"
            + "    ad_mixranking_sum_f     bigint,\n"
            + "    ad_mixranking_sum_fn    bigint,\n"
            + "    ad_adranking_sum_dn     bigint,\n"
            + "    ad_mixranking_avg_abp   float,\n"
            + "    ad_mixranking_avg_apctr float,\n"
            + "    ad_adranking_avg_aecpm  float,\n"
            + "    ad_adranking_sum_wn     bigint,\n"
            + "    ad_adranking_sum_in     bigint,\n"
            + "    ad_adranking_sum_cn     bigint,\n"
            + "    ad_adranking_sum_dc     bigint,\n"
            + "    ad_mixranking_sum_spctr double,\n"
            + "    ad_mixranking_sum_cpctr bigint,\n"
            + "    ad_mixranking_sum_sptr  double,\n"
            + "    ad_mixranking_sum_cptr  bigint,\n"
            + "    ad_mixranking_sum_sbp   double,\n"
            + "    ad_mixranking_sum_cbp   bigint,\n"
            + "    ad_ranking_sum_secpm    double,\n"
            + "    ad_ranking_sum_cecpm    bigint,\n"
            + "    ad_ranking_sum_srbwp    double,\n"
            + "    ad_ranking_sum_crbwp    bigint,\n"
            + "    ad_ranking_sum_sorder   bigint,\n"
            + "    ad_ranking_sum_corder   bigint,\n"
            + "    ad_tracking_sum_dybn    double \n"
            + ") WITH (\n"
            + "    'connector' = 'kafka', \n"
            + "    'topic' = 'dm_adplatform_ad_play_link_inc_s', \n"
            + "    'properties.bootstrap.servers' = 'xxx:xxx', \n"
            + "    'properties.group.id' = '16134c114ebd4477be20e9c8c4925a48_dm_adplatform_ad_play_link_inc_s', \n"
            + "    'format' = 'json', \n"
            + "    'json.ignore-parse-errors' = 'true', \n"
            + "    'scan.startup.mode' = 'latest-offset', \n" +
            "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
            "  'properties.sasl.mechanism' = 'PLAIN',\n" +
            "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"xxx\" password=\"xxx\";',\n"
            + "    'scan.topic-partition-discovery.interval' = '10000'\n"
            + ");";
    }

    private static String createPGTableSQL() {
        return "\n"
            + "create table dm_adplatform_ad_play_link_inc_s_pg\n"
            + "(\n"
            + "    pt_d                    string,\n"
            + "    pt_h                    string,\n"
            + "    pt_m                    string,\n"
            + "    mediaid                 string,\n"
            + "    adunitid                string,\n"
            + "    billingtype             string,\n"
            + "    advertiserid            string,\n"
            + "    advertisertype          string,\n"
            + "    adgroupid               string,\n"
            + "    adcreativeid            string,\n"
            + "    promotionpurpose        string,\n"
            + "    ad_mixranking_sum_m     bigint,\n"
            + "    ad_mixranking_sum_r     bigint,\n"
            + "    ad_mixranking_avg_aptr  float,\n"
            + "    ad_mixranking_sum_fi    bigint,\n"
            + "    ad_mixranking_sum_fu    bigint,\n"
            + "    ad_mixranking_sum_f     bigint,\n"
            + "    ad_mixranking_sum_fn    bigint,\n"
            + "    ad_adranking_sum_dn     bigint,\n"
            + "    ad_mixranking_avg_abp   float,\n"
            + "    ad_mixranking_avg_apctr float,\n"
            + "    ad_adranking_avg_aecpm  float,\n"
            + "    ad_adranking_sum_wn     bigint,\n"
            + "    ad_adranking_sum_in     bigint,\n"
            + "    ad_adranking_sum_cn     bigint,\n"
            + "    ad_adranking_sum_dc     bigint,\n"
            + "    ad_mixranking_sum_spctr double,\n"
            + "    ad_mixranking_sum_cpctr bigint,\n"
            + "    ad_mixranking_sum_sptr  double,\n"
            + "    ad_mixranking_sum_cptr  bigint,\n"
            + "    ad_mixranking_sum_sbp   double,\n"
            + "    ad_mixranking_sum_cbp   bigint,\n"
            + "    ad_ranking_sum_secpm    double,\n"
            + "    ad_ranking_sum_cecpm    bigint,\n"
            + "    ad_ranking_sum_srbwp    double,\n"
            + "    ad_ranking_sum_crbwp    bigint,\n"
            + "    ad_ranking_sum_sorder   bigint,\n"
            + "    ad_ranking_sum_corder   bigint,\n"
            + "    ad_tracking_sum_dybn    double,\n"
            + "    primary key (pt_d, pt_h, pt_m, mediaid, adunitid, billingtype, advertiserid, advertisertype,\n"
            + "                 adgroupid, adcreativeid, promotionpurpose) NOT ENFORCED\n"
            + ")  WITH (\n"
            + "    'connector' = 'jdbcPG',\n"
            + "    'url' = 'jdbc:postgresql://xxx:9000/rongyao?currentSchema=public&reWriteBatchedInserts=true',\n"
            + "    'username' = 'dbadmin',\n"
            + "    'password' = 'xxx',\n"
            + "    'table-name' = 'dm_adplatform_ad_play_link_inc_s',\n"
            + "    'sink.buffer-flush.max-rows' = '10000',\n"
            + "    'sink.buffer-flush.interval' = '10s' ,\n"
            + "    'write-mode' = 'copy'\n"
            + ");";
    }

    private static String insertSQL() {
        return
            "insert into dm_adplatform_ad_play_link_inc_s_pg \n"
                + "select pt_d,\n"
                + "       pt_h,\n"
                + "       pt_m,\n"
                + "       mediaid,\n"
                + "       adunitid,\n"
                + "       billingtype,\n"
                + "       advertiserid,\n"
                + "       advertisertype,\n"
                + "       adgroupid,\n"
                + "       adcreativeid,\n"
                + "       promotionpurpose,\n"
                + "       ad_mixranking_sum_m,\n"
                + "       ad_mixranking_sum_r,\n"
                + "       ad_mixranking_avg_aptr,\n"
                + "       ad_mixranking_sum_fi,\n"
                + "       ad_mixranking_sum_fu,\n"
                + "       ad_mixranking_sum_f,\n"
                + "       ad_mixranking_sum_fn,\n"
                + "       ad_adranking_sum_dn,\n"
                + "       ad_mixranking_avg_abp,\n"
                + "       ad_mixranking_avg_apctr,\n"
                + "       ad_adranking_avg_aecpm,\n"
                + "       ad_adranking_sum_wn,\n"
                + "       ad_adranking_sum_in,\n"
                + "       ad_adranking_sum_cn,\n"
                + "       ad_adranking_sum_dc,\n"
                + "       ad_mixranking_sum_spctr,\n"
                + "       ad_mixranking_sum_cpctr,\n"
                + "       ad_mixranking_sum_sptr,\n"
                + "       ad_mixranking_sum_cptr,\n"
                + "       ad_mixranking_sum_sbp,\n"
                + "       ad_mixranking_sum_cbp,\n"
                + "       ad_ranking_sum_secpm,\n"
                + "       ad_ranking_sum_cecpm,\n"
                + "       ad_ranking_sum_srbwp,\n"
                + "       ad_ranking_sum_crbwp,\n"
                + "       ad_ranking_sum_sorder,\n"
                + "       ad_ranking_sum_corder,\n"
                + "       ad_tracking_sum_dybn\n"
                + "from dm_adplatform_ad_play_link_inc_s_kafka;";
    }

}


