package com.tencent.flink.java;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.LongAdder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class WritePublicKafkaExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers("xxx:9092")
            .setProperty("security.protocol", "SASL_PLAINTEXT")
            .setProperty("sasl.mechanism", "PLAIN")
            .setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"xxx\" password=\"xxx\";")
            .setProperty("transaction.timeout.ms", 5 * 60 * 1000 + "")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("dm_adplatform_ad_play_link_inc_s")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();

        env.addSource(new RichSourceFunction<String>() {

            private JSONObject jsonTemp;
            private LongAdder longAdder;

            @Override
            public void open(Configuration parameters) throws Exception {
                byte[] bytes = Files.readAllBytes(Paths.get(
                    this.getClass()
                        .getClassLoader()
                        .getResource("value_2.json")
                        .getPath()));
                String jsonValue = new String(bytes, StandardCharsets.UTF_8);
                jsonTemp = JSON.parseObject(jsonValue);

                longAdder = new LongAdder();
            }

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (true) {
                    jsonTemp.put("mediaid", System.nanoTime() + "");
                    sourceContext.collect(jsonTemp.toString());
                    longAdder.increment();
                    if (longAdder.longValue() % 10000 == 0) {
                        Thread.sleep(500);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).sinkTo(kafkaSink);

        env.execute();
    }

    private static String value(){
        return "{\n"
            + "  \"pt_d\": \"20240419\",\n"
            + "  \"pt_h\": \"16\",\n"
            + "  \"pt_m\": \"30\",\n"
            + "  \"mediaid\": \"1751790186440294400\",\n"
            + "  \"adunitid\": \"1751887740662317056\",\n"
            + "  \"billingtype\": \"1\",\n"
            + "  \"advertiserid\": \"122659\",\n"
            + "  \"advertisertype\": \"1\",\n"
            + "  \"adgroupid\": \"10002809753\",\n"
            + "  \"adcreativeid\": \"10002809803\",\n"
            + "  \"promotionpurpose\": \"0\",\n"
            + "  \"ad_mixranking_sum_m\": 455,\n"
            + "  \"ad_mixranking_sum_r\": 342,\n"
            + "  \"ad_mixranking_avg_aptr\": 0.0,\n"
            + "  \"ad_mixranking_sum_fi\": 0,\n"
            + "  \"ad_mixranking_sum_fu\": 0,\n"
            + "  \"ad_mixranking_sum_f\": 0,\n"
            + "  \"ad_mixranking_sum_fn\": 0,\n"
            + "  \"ad_adranking_sum_dn\": 2,\n"
            + "  \"ad_mixranking_avg_abp\": 0.0,\n"
            + "  \"ad_mixranking_avg_apctr\": 0.0,\n"
            + "  \"ad_adranking_avg_aecpm\": 0.0,\n"
            + "  \"ad_adranking_sum_wn\": 0,\n"
            + "  \"ad_adranking_sum_in\": 0,\n"
            + "  \"ad_adranking_sum_cn\": 0,\n"
            + "  \"ad_adranking_sum_dc\": 0,\n"
            + "  \"ad_mixranking_sum_spctr\": 3.0358209999999985,\n"
            + "  \"ad_mixranking_sum_cpctr\": 346,\n"
            + "  \"ad_mixranking_sum_sptr\": null,\n"
            + "  \"ad_mixranking_sum_cptr\": null,\n"
            + "  \"ad_mixranking_sum_sbp\": 34.600000000000016,\n"
            + "  \"ad_mixranking_sum_cbp\": 346,\n"
            + "  \"ad_ranking_sum_secpm\": 0.2,\n"
            + "  \"ad_ranking_sum_cecpm\": 2,\n"
            + "  \"ad_ranking_sum_srbwp\": 3.1799999999999997,\n"
            + "  \"ad_ranking_sum_crbwp\": 2,\n"
            + "  \"ad_ranking_sum_sorder\": 0,\n"
            + "  \"ad_ranking_sum_corder\": 0,\n"
            + "  \"ad_tracking_sum_dybn\": 0.0\n"
            + "}";
    }


}
