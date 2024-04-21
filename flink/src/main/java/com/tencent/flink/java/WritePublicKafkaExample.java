package com.tencent.flink.java;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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
            .setBootstrapServers("ip:host")
            .setProperty("security.protocol", "SASL_PLAINTEXT")
            .setProperty("sasl.mechanism", "PLAIN")
            .setProperty("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"xxx\" password=\"xxx\";")
            .setProperty("transaction.timeout.ms", 5 * 60 * 1000 + "")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("lizy_test_topic")
                .setValueSerializationSchema(new SimpleStringSchema())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();

        env.addSource(new RichSourceFunction<String>() {

            private JSONObject jsonTemp;

            @Override
            public void open(Configuration parameters) throws Exception {
                byte[] bytes = Files.readAllBytes(Paths.get(
                    this.getClass()
                        .getClassLoader()
                        .getResource("value_1.json")
                        .getPath()));
                String jsonValue = new String(bytes, StandardCharsets.UTF_8);
                jsonTemp = JSON.parseObject(jsonValue);
            }

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                sourceContext.collect(jsonTemp.toString());
            }

            @Override
            public void cancel() {

            }
        }).sinkTo(kafkaSink);

        env.execute();
    }


}
