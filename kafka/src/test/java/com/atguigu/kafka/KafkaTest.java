package com.atguigu.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Future;

public class KafkaTest {

    //测试：kafka produce
    @Test
    public void test01() throws Exception {
        String key = "key01";
        String value = "value01";
        Future<RecordMetadata> future1 = KafkaProducertTest.sendMessage01(key, value);
        Future<RecordMetadata> future2 = KafkaProducertTest.sendMessage01(key, value);
        System.out.println("offset : " + future1.get().offset());
        System.out.println("partition : " + future1.get().partition());
        System.out.println("=====================================");
        System.out.println("offset : " + future2.get().offset());
        System.out.println("partition : " + future2.get().partition());
        KafkaProducertTest.close();
    }

    //测试：kafka consumer
    @Test
    public void test02() {
        KafkaConsumerUtil.consumeMessage01();
    }

    //测试：kafka consumer
    @Test
    public void test03() {
        KafkaConsumerUtil.consumeMessage02();
    }
}
