package com.tencent.ckafka;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaTransactionConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "1:9092");
        props.put("group.id", "test_3");
        props.put("session.timeout.ms", 30000);       // 如果其超时，将会可能触发rebalance并认为已经死去，重新选举Leader
        props.put("enable.auto.commit", "false");      // 开启自动提交
        props.put("auto.commit.interval.ms", "1000"); // 自动提交时间
        props.put("auto.offset.reset", "earliest"); // 从最早的offset开始拉取，latest:从最近的offset开始消费
        props.put("client.id", "producer-syn-1"); // 发送端id,便于统计
        props.put("max.poll.records", "100"); // 每次批量拉取条数
        props.put("max.poll.interval.ms", "1000");
        props.put("isolation.level", "read_committed"); // 设置隔离级别

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props,
            new StringDeserializer(), new StringDeserializer());

        consumer.subscribe(Collections.singletonList("test_kafka"));

//        for ()


    }

}
