package com.atguigu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerUtil {
    private static KafkaConsumer<String, String> consumer;

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.213.101:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");//是否自动提交offset
        //props.put("auto.commit.interval.ms", "1000");//自动提交offset的时间间隔
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(KafkaProducertTest.KAFKA_TOPIC_NAME));
    }

    //消费record，手动异步提交offset
    public static void consumeMessage01() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

            //异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("Commit failed for" + offsets);
                    }
                }
            });
        }
    }

    //消费record，手动同步提交offset
    public static void consumeMessage02() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            consumer.commitSync();
        }
    }

}
