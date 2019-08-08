package com.atguigu.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

//自定义存储offset
public class KafkaConsumerUtil2 {
    private static KafkaConsumer<String, String> consumer;
    private static Map<TopicPartition, Long> currentOffset = new HashMap<>();

    static {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.213.101:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");//是否自动提交offset
        //props.put("auto.commit.interval.ms", "1000");//自动提交offset的时间间隔
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(KafkaProducertTest.KAFKA_TOPIC_NAME), new ConsumerRebalanceListener() {
            @Override //该方法会在Rebalance之前调用
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                commitOffset(currentOffset);
            }

            @Override //该方法会在Rebalance之后调用
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                currentOffset.clear();
                for (TopicPartition partition : partitions) {
                    consumer.seek(partition, getOffset(partition));//定位到最近提交的offset位置继续消费
                }
            }
        });
    }

    //消费record，手动异步提交offset
    public static void consumeMessage01() {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);//消费者拉取数据
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                currentOffset.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            commitOffset(currentOffset);//异步提交
        }
    }

    //获取某分区的最新offset
    private static long getOffset(TopicPartition partition) {
        return 0;
    }

    //提交该消费者所有分区的offset
    private static void commitOffset(Map<TopicPartition, Long> currentOffset) {

    }

}
