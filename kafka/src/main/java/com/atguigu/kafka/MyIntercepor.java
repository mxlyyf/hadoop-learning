package com.atguigu.kafka;


import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class MyIntercepor implements ProducerInterceptor<String, String> {
    private int successCount = 0;
    private int errorCount = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord(record.topic(),
                record.partition(),
                record.timestamp(),
                record.key(),
                System.currentTimeMillis() + "," + record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) successCount++;
        else errorCount++;
    }

    @Override
    public void close() {
        //打印结果
        System.out.println("发送消息成功数：" + successCount);
        System.out.println("发送消息失败数：" + errorCount);
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
