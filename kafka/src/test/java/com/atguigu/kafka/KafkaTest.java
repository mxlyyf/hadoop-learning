package com.atguigu.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Future;

public class KafkaTest {
    @Test
    public void test01() throws Exception {
        String key = "key01";
        String value = "value01";
        Future<RecordMetadata> future = KafkaProducer.sendMessage01(key, value);

        Assert.assertTrue("true", future.isDone());
        System.out.println(future.get().offset());
    }

    @Test
    public void test02() {
        KafkaConsumerUtil.consume01();
    }
}
