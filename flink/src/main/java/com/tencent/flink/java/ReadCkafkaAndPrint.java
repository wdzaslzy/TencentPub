package com.tencent.flink.java;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.JaasUtils;

public class ReadCkafkaAndPrint {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM,
            "/Users/rodenli/Workspace/wdzaslzy/TencentPub/ckafka/src/main/resources/ckafka_client_jaas.conf");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
            .setBootstrapServers("119.29.50.160:50000")
            .setTopics("lizy_test")
            .setGroupId("lizy_consumer_group")
            .setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
            .setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema()).build();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers("")
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic("topic-name")
                    .setValueSerializationSchema(new SimpleStringSchema())
                    .build()
            )
            .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        DataStreamSource<String> kafkaSourceStream = env.fromSource(kafkaSource,
            WatermarkStrategy.noWatermarks(), "Kafka_Source");

        kafkaSourceStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] splits = value.split("_");
                return splits[0] + "_" + Integer.parseInt(splits[1]) * 2;
            }
        }).print();

        env.execute("KafkaDemo");
    }

}