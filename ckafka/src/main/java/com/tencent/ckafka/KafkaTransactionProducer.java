package com.tencent.ckafka;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaTransactionProducer {

    public static void main(String[] args) {
        //加载kafka.properties。
        Properties kafkaProperties = CKafkaConfigure.getCKafkaProperties();
        //设置 jaas 配置信息
        System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, kafkaProperties.getProperty("java.security.auth.login.config"));


        Properties props = new Properties();
        props.put("bootstrap.servers", "106.55.75.94:50000");
        props.put("transactional.id", "my-transactional-id");
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        //  SASL 采用 Plain 方式。
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(),
            new StringSerializer());

        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                producer.send(
                    new ProducerRecord<>("test_lizy", Integer.toString(i), Integer.toString(i)));
            }
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
    }

}
