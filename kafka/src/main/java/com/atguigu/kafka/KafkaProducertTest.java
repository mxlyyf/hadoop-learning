package com.atguigu.kafka;

import kafka.server.KafkaConfig;
import org.apache.kafka.clients.producer.*;
import org.omg.PortableInterceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducertTest {
    public static final String KAFKA_TOPIC_NAME = "first";

    public static Producer<String, String> producer;

    static {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.213.101:9092");//kafka集群，broker-list
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("retries", 1); //重试次数
        props.put("batch.size", 16384); //批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小 32M
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //加入自定义拦截器
        List<String> interceptors = new ArrayList<>();
        interceptors.add("com.atguigu.kafka.MyIntercepor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        producer = new KafkaProducer(props);
    }

    public static Future<RecordMetadata> sendMessage01(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord(KAFKA_TOPIC_NAME, key, value);
        Future<RecordMetadata> future = producer.send(record);
        return future;
    }

    public static void sendMessage02(String key, String value) {
        producer.send(new ProducerRecord<String, String>(KAFKA_TOPIC_NAME, key, value), new Callback() {
            //回调函数，该方法会在Producer收到ack时调用，为异步调用
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    System.out.println("success->" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            }
        });
    }

    public static void close() {
        producer.close();
    }


}
