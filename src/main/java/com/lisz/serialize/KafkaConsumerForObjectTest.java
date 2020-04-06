package com.lisz.serialize;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

// 同一个组之内的消息是有序的，有consumer加入或者退出，可能引起各个consumer对于topic下各个partition的rebalance。有可能一个consumer负责两个partition
public class KafkaConsumerForObjectTest {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "Kafka_1:9092,Kafka_2:9092,Kafka_3:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ObjectDeserializer.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");//可以用到组管理机制：组内负载均衡，组间广播

        KafkaConsumer<Integer, Person> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile("^topic.*"));//可以用到组管理机制：组内负载均衡，组间广播
        while (true) {
            ConsumerRecords<Integer, Person> consumerRecords = consumer.poll(Duration.ofSeconds(1));
            if (!consumerRecords.isEmpty()) { //从队列中取到了数据
                Iterable<ConsumerRecord<Integer, Person>> records = consumerRecords.records("topic01");//也可以用consumerRecords.iterator()
                records.forEach(r->{
                    System.out.println(String.format("Topic: %s Partition: %s Offset: %s Key: %s Person: %s Timestamp: %s",
                            r.topic(), r.partition(), r.offset(), r.key(), r.value(), r.timestamp()));
                });
            }
        }
    }
}